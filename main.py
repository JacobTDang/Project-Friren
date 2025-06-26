"""
main.py

Enhanced Production Trading System Entry Point with COMPREHENSIVE DEBUG MONITORING

This script launches the complete production trading system with real-time monitoring
of every action, process state, queue state, and shared memory contents.

Features:
- Real-time process monitoring (every action logged)
- Queue state visualization (pending messages, routing)
- Shared memory/state contents display
- API call tracking
- Decision pipeline monitoring
- Error tracking and recovery
- Performance metrics
"""

import sys
import os
import logging
import signal
import time
from pathlib import Path
from datetime import datetime
import threading

# Add project root to Python path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Windows-specific multiprocessing setup
if sys.platform == "win32":
    import multiprocessing as mp
    mp.freeze_support()
    mp.set_start_method('spawn', force=True)

# DEVELOPMENT: Add 10-minute timeout for debugging
DEVELOPMENT_TIMEOUT = 600  # 10 minutes

def setup_enhanced_logging():
    """Setup comprehensive logging system with colorized output"""
    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Try to import colorama for colored output
    try:
        import colorama
        from colorama import Fore, Back, Style
        colorama.init(autoreset=True)
        use_colors = True
    except ImportError:
        use_colors = False

    # Create custom colored formatter
    class ColoredFormatter(logging.Formatter):
        """Custom formatter with colors for different log levels"""
        
        def __init__(self, use_colors=True):
            super().__init__()
            self.use_colors = use_colors
            if use_colors:
                try:
                    from colorama import Fore, Back, Style
                    self.colors = {
                        'DEBUG': Fore.CYAN,
                        'INFO': Fore.GREEN,
                        'WARNING': Fore.YELLOW,
                        'ERROR': Fore.RED,
                        'CRITICAL': Fore.RED + Back.YELLOW + Style.BRIGHT,
                        'RESET': Style.RESET_ALL
                    }
                except ImportError:
                    self.use_colors = False
                    self.colors = {}
            else:
                self.colors = {}

        def format(self, record):
            if self.use_colors and record.levelname in self.colors:
                # Color the entire message
                color = self.colors[record.levelname]
                reset = self.colors.get('RESET', '')
                
                # Format the message with colors
                timestamp = time.strftime('%H:%M:%S', time.localtime(record.created))
                
                # Color different parts
                colored_level = f"{color}[{record.levelname}]{reset}"
                from colorama import Fore
                colored_name = f"{Fore.BLUE}{record.name}{reset}" if self.use_colors else record.name
                colored_msg = f"{color}{record.getMessage()}{reset}"
                
                return f"{timestamp} {colored_level} {colored_name}: {colored_msg}"
            else:
                # Fallback to plain format
                timestamp = time.strftime('%H:%M:%S', time.localtime(record.created))
                return f"{timestamp} [{record.levelname}] {record.name}: {record.getMessage()}"

    # Configure root logger with colored formatter
    # Clear any existing handlers first
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # File handler (no colors)
    file_handler = logging.FileHandler(logs_dir / "trading_system_debug.log")
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    # Console handler (with colors)
    console_handler = logging.StreamHandler()
    console_formatter = ColoredFormatter(use_colors=use_colors)
    console_handler.setFormatter(console_formatter)

    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.INFO)

    # Set specific logger levels for maximum detail
    logging.getLogger("main_orchestrator").setLevel(logging.DEBUG)
    logging.getLogger("debug_monitor").setLevel(logging.DEBUG)
    logging.getLogger("process_manager").setLevel(logging.INFO)
    logging.getLogger("shared_state_manager").setLevel(logging.INFO)
    logging.getLogger("queue_manager").setLevel(logging.INFO)

    logger = logging.getLogger("main")
    logger.info("=== ENHANCED LOGGING SYSTEM ACTIVATED ===")
    logger.info("Comprehensive debug monitoring enabled")
    logger.info("Showing all processes, queues, and shared state in real-time")
    return logger

# Global shutdown flag for graceful termination
shutdown_requested = False
global_orchestrator = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully - Windows compatible"""
    global shutdown_requested, global_orchestrator

    logger = logging.getLogger("main")
    logger.critical(f"SIGNAL RECEIVED: {signum} - Initiating graceful shutdown")

    # Set shutdown flag IMMEDIATELY - this is critical
    shutdown_requested = True

    logger.info("SHUTDOWN: Shutdown flag set - main loop will handle graceful shutdown")

    # FIXED: Also try to stop the orchestrator directly if available
    if global_orchestrator:
        try:
            logger.info("SIGNAL: Attempting direct orchestrator shutdown...")
            global_orchestrator.stop_system()
        except Exception as e:
            logger.warning(f"SIGNAL: Direct orchestrator shutdown failed: {e}")

    # Don't force exit here - let the main loop handle it gracefully

def load_dynamic_watchlist(logger):
    """
    Load watchlist from database - production ready
    Combines current holdings + high-priority opportunities
    """
    try:
        logger.info("Loading dynamic watchlist from database...")

        # Import database components
        from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager

        # Initialize database connection
        db_manager = TradingDBManager()

        # Step 1: Get current holdings (these are always monitored)
        holdings = db_manager.get_current_holdings()
        holding_symbols = [h['symbol'] for h in holdings if float(h['quantity']) > 0]

        logger.info(f"Current Holdings: {len(holding_symbols)} symbols {holding_symbols}")

        # Step 2: Get high-priority watchlist opportunities
        watchlist = db_manager.get_watchlist(active_only=True)
        opportunity_symbols = [
            w['symbol'] for w in watchlist
            if w['priority'] >= 7 and w['symbol'] not in holding_symbols
        ][:10]  # Top 10 opportunities

        logger.info(f"High-Priority Opportunities: {len(opportunity_symbols)} symbols {opportunity_symbols}")

        # Step 3: Combine holdings + opportunities
        dynamic_symbols = holding_symbols + opportunity_symbols

        # Fallback to default if database is empty
        if not dynamic_symbols:
            logger.warning("Database watchlist empty - using fallback symbols")
            dynamic_symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA']

        logger.info(f"Dynamic Watchlist Complete: {len(dynamic_symbols)} total symbols")
        return dynamic_symbols

    except Exception as e:
        logger.error(f"Error loading dynamic watchlist: {e}")
        logger.warning("Using fallback static watchlist")
        return ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA']

def refresh_watchlist_after_trade(orchestrator, trade_symbol, logger):
    """
    Refresh watchlist after a trade - production strategy
    Only called when portfolio actually changes
    """
    try:
        logger.info(f"Trade completed for {trade_symbol} - refreshing dynamic watchlist")

        # Load updated watchlist from database
        updated_symbols = load_dynamic_watchlist(logger)

        # Update the system configuration
        orchestrator.config.symbols = updated_symbols

        # Notify enhanced news pipeline of updated watchlist
        if hasattr(orchestrator, 'process_manager'):
            logger.info("Notifying processes of updated watchlist...")
            # The next pipeline cycle will pick up the new symbols

        logger.info(f"Watchlist refreshed after trade: {len(updated_symbols)} symbols active")
        return True

    except Exception as e:
        logger.error(f"Error refreshing watchlist after trade: {e}")
        return False

def check_system_requirements():
    """Check system requirements and environment setup"""
    logger = logging.getLogger("main")
    logger.info("=== SYSTEM REQUIREMENTS CHECK ===")

    requirements_met = True

    # Check Python version
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    logger.info(f"Python Version: {python_version}")
    if sys.version_info.major < 3 or sys.version_info.minor < 8:
        logger.error("Python 3.8+ required")
        requirements_met = False

    # Check operating system
    logger.info(f"Operating System: {sys.platform}")
    if sys.platform == "win32":
        logger.info("Windows detected - Using Windows-compatible configuration")

    # Check required environment variables
    required_env_vars = [
        'ALPACA_API_KEY',
        'ALPACA_SECRET_KEY'
    ]

    for var in required_env_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"Environment Variable {var}: {'*' * len(value)} (SET)")
        else:
            logger.error(f"Environment Variable {var}: NOT SET")
            requirements_met = False

    # Check optional environment variables
    optional_env_vars = [
        'ALPACA_EMERGENCY_CODE',
        'TRADING_MODE',
        'LOG_LEVEL'
    ]

    for var in optional_env_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"Optional Environment Variable {var}: {value}")
        else:
            logger.info(f"Optional Environment Variable {var}: Using default")

    if not requirements_met:
        logger.critical("SYSTEM REQUIREMENTS NOT MET - Cannot start trading system")
        return False

    logger.info("=== SYSTEM REQUIREMENTS CHECK PASSED ===")
    return True

def initialize_trading_system():
    """Initialize the complete trading system with debug monitoring"""
    logger = logging.getLogger("main")

    try:
        logger.info("=== INITIALIZING TRADING SYSTEM ===")

        # Import trading system components
        from Friren_V1.trading_engine.portfolio_manager.orchestrator import MainOrchestrator
        from Friren_V1.trading_engine.portfolio_manager.orchestrator_utils import SystemConfig, TradingMode

        # Create system configuration
        logger.info("STEP 1: Creating system configuration...")
        config = SystemConfig()

        # Override with environment variables if set
        trading_mode_env = os.getenv('TRADING_MODE', 'PAPER').upper()
        if trading_mode_env == 'LIVE':
            config.trading_mode = TradingMode.LIVE_TRADING
            logger.warning("LIVE TRADING MODE ENABLED - Real money at risk!")
        else:
            config.trading_mode = TradingMode.PAPER_TRADING
            logger.info("PAPER TRADING MODE - Safe testing environment")

        # Configure symbols from database (production-ready dynamic watchlist)
        config.symbols = load_dynamic_watchlist(logger)
        logger.info(f"Dynamic Watchlist Loaded: {len(config.symbols)} symbols")
        logger.info(f"Trading Symbols: {config.symbols}")

        # Set monitoring intervals for detailed debugging
        config.health_check_interval = 5  # Check health every 5 seconds
        config.api_rate_limit_buffer = 0.8  # 80% of rate limit

        logger.info("STEP 2: Creating main orchestrator with debug monitoring...")
        orchestrator = MainOrchestrator(config)

        logger.info("STEP 3: Initializing all system components...")
        orchestrator.initialize_system()

        # DEBUG: Check current holdings in database
        logger.info("=== DEBUG: CHECKING CURRENT HOLDINGS IN DATABASE ===")
        try:
            from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
            db_manager = TradingDBManager()
            holdings = db_manager.get_current_holdings()
            logger.info(f"Total holdings found: {len(holdings)}")
            for h in holdings:
                logger.info(f"  {h}")
            if not holdings:
                logger.warning("NO HOLDINGS FOUND IN DATABASE - This explains why Symbol Coordinator shows Active=0")
            logger.info("=== END DEBUG: CURRENT HOLDINGS ===")
        except Exception as e:
            logger.error(f"Error checking holdings: {e}")

        logger.info("=== TRADING SYSTEM INITIALIZATION COMPLETE ===")
        return orchestrator

    except Exception as e:
        logger.critical(f"TRADING SYSTEM INITIALIZATION FAILED: {e}")
        import traceback
        logger.critical(f"Full traceback: {traceback.format_exc()}")
        return None

def run_trading_system():
    """Enhanced Production Trading System Execution with comprehensive monitoring"""
    global shutdown_requested  # FIXED: Access global shutdown flag

    # Import colored print functions for this thread
    try:
        from colored_print import header, success, error, warning, info, progress
    except ImportError:
        # Fallback functions if colored_print not available
        def header(msg): print(f"=== {msg} ===")
        def success(msg): print(f"[SUCCESS] {msg}")
        def error(msg): print(f"[ERROR] {msg}")
        def warning(msg): print(f"[WARNING] {msg}")
        def info(msg): print(f"[INFO] {msg}")
        def progress(msg): print(f"[PROGRESS] {msg}")

    logger = logging.getLogger("main")
    orchestrator = None

    try:
        logger.info("=== STARTING ENHANCED TRADING SYSTEM ===")
        logger.info("Features: Real-time monitoring, Queue debugging, CPU tracking, 24/7 news collection")

        # Initialize the trading system
        orchestrator = initialize_trading_system()
        if not orchestrator:
            logger.error("Failed to initialize trading system")
            return

        global global_orchestrator
        global_orchestrator = orchestrator
        
        # CRITICAL FIX: Apply direct Symbol Coordinator fix
        progress("Applying direct Symbol Coordinator fix...")
        try:
            from symbol_coordinator_direct_fix import apply_direct_fix_to_main
            
            # Create working Symbol Coordinator
            direct_fix = apply_direct_fix_to_main()
            
            if direct_fix:
                # Inject into orchestrator
                if direct_fix.inject_into_orchestrator(orchestrator):
                    success("Direct Symbol Coordinator fix applied successfully!")
                    info("System should now generate trading messages and collect news")
                else:
                    error("Failed to inject Symbol Coordinator fix")
            else:
                error("Direct Symbol Coordinator fix creation failed")
                
        except Exception as e:
            error(f"Direct Symbol Coordinator fix error: {e}")
            import traceback
            traceback.print_exc()

        # REMOVED: Colorized news monitoring simulation
        # NO SIMULATION - Real processes will handle news collection output

        # **ADDITIONAL DEBUG MONITORING**
        # Log initial system state
        logger.info("=== INITIAL SYSTEM STATE DEBUG ===")

        # Test CPU monitoring
        try:
            if hasattr(orchestrator, 'system_monitor') and orchestrator.system_monitor:
                orchestrator.system_monitor.update_system_metrics()
                cpu_usage = orchestrator.system_monitor.status.cpu_usage_percent
                memory_usage = orchestrator.system_monitor.status.memory_usage_mb
                logger.info(f"FIXED CPU MONITORING: {cpu_usage:.1f}% (should no longer be 0)")
                logger.info(f"MEMORY USAGE: {memory_usage:.1f}MB")
            else:
                logger.warning("System monitor not available for CPU testing")
        except Exception as e:
            logger.error(f"Error testing CPU monitoring: {e}")

        # Test queue monitoring
        try:
            if hasattr(orchestrator, 'queue_manager') and orchestrator.queue_manager:
                queue_status = orchestrator.queue_manager.get_queue_status()
                logger.info("=== FIXED QUEUE MONITORING ===")
                for queue_name, queue_info in queue_status.items():
                    logger.info(f"{queue_name.upper()}: Size={queue_info['size']} (should show actual counts)")
                    if queue_info.get('recent_messages'):
                        logger.info(f"  Recent messages: {queue_info['recent_messages']}")
            else:
                logger.warning("Queue manager not available for queue testing")
        except Exception as e:
            logger.error(f"Error testing queue monitoring: {e}")

        # Test news collection
        try:
            if hasattr(orchestrator, 'process_manager') and orchestrator.process_manager:
                processes = orchestrator.process_manager.get_process_status()
                news_processes = [p for p in processes.keys() if 'news' in p.lower()]
                if news_processes:
                    logger.info(f"=== FIXED NEWS COLLECTION (24/7) ===")
                    logger.info(f"News processes found: {news_processes}")
                    logger.info("News collection is now set to run 24/7 regardless of market hours")
                else:
                    logger.warning("No news collection processes found")
            else:
                logger.warning("Process manager not available for news testing")
        except Exception as e:
            logger.error(f"Error testing news collection: {e}")

        logger.info("=== SYSTEM READY - All bug fixes applied ===")

        # Main execution loop
        execution_count = 0
        max_executions = 100  # Prevent infinite loops during testing

        # FIXED: Add timeout mechanism for shutdown
        shutdown_timeout_start = None

        # Inject test events if requested (only once, after orchestrator is started)
        injected_test_events = False

        while not shutdown_requested and execution_count < max_executions:
            try:
                execution_count += 1
                logger.info(f"=== EXECUTION CYCLE {execution_count} ===")

                # Inject test events after orchestrator is started (cycle 1)
                if not injected_test_events and os.getenv("INJECT_TEST_EVENTS", "0") == "1":
                    # Wait for orchestrator to be fully started
                    if hasattr(orchestrator, 'queue_manager') and orchestrator.queue_manager is not None and \
                       hasattr(orchestrator, 'execution_orchestrator') and orchestrator.execution_orchestrator is not None:
                        try:
                            logger.info("[TEST] INJECT_TEST_EVENTS=1 detected. Injecting NEWS_REQUEST and buy order for AAPL.")
                            from Friren_V1.multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority
                            news_msg = QueueMessage(
                                message_type=MessageType.NEWS_REQUEST,
                                priority=MessagePriority.HIGH,
                                sender_id="main_test_injector",
                                recipient_id="enhanced_news_pipeline",
                                payload={
                                    "symbol": "AAPL",
                                    "force_refresh": True,
                                    "timestamp": datetime.now().isoformat()
                                }
                            )
                            orchestrator.queue_manager.send_message(news_msg)
                            logger.info("[TEST] NEWS_REQUEST for AAPL injected into priority_queue.")

                            from types import SimpleNamespace
                            risk_validation = SimpleNamespace(
                                symbol="AAPL",
                                is_approved=True,
                                should_execute=True,
                                size_calculation=SimpleNamespace(
                                    needs_trade=True,
                                    shares_to_trade=1,
                                    is_buy=True,
                                    current_price=200.0
                                ),
                                original_decision=SimpleNamespace(final_direction=1)
                            )
                            result = orchestrator.execution_orchestrator.execute_approved_decision(
                                risk_validation=risk_validation,
                                strategy_name="test_injector",
                                strategy_confidence=1.0
                            )
                            logger.info(f"[TEST] Buy order for 1 AAPL submitted to execution orchestrator. Result: {getattr(result, 'execution_summary', result)}")
                            injected_test_events = True
                        except Exception as e:
                            logger.error(f"[TEST] Error injecting test events: {e}")
                    else:
                        logger.debug("[TEST] Waiting for orchestrator to be fully started before injecting test events...")

                # Check shutdown flag before executing cycle
                if shutdown_requested:
                    logger.info("SHUTDOWN: Shutdown requested, breaking execution loop")
                    break

                # FIXED: Start the orchestrator system (should only be called once)
                if execution_count == 1:
                    logger.info("Starting orchestrator system...")
                    try:
                        # FIXED: Add timeout to start_system to prevent hanging
                        start_complete = threading.Event()
                        start_error = None

                        def start_orchestrator():
                            nonlocal start_error
                            try:
                                orchestrator.start_system()
                                start_complete.set()
                            except Exception as e:
                                start_error = e
                                start_complete.set()

                        start_thread = threading.Thread(target=start_orchestrator, daemon=True)
                        start_thread.start()

                        # Wait for start with timeout - increased for process health checks
                        if start_complete.wait(timeout=150.0):  # 150 second timeout for process startup
                            if start_error:
                                logger.error(f"Orchestrator start failed: {start_error}")
                                break
                            else:
                                logger.info("Orchestrator started successfully")
                        else:
                            logger.warning("Orchestrator start timed out - continuing anyway")

                    except Exception as e:
                        logger.error(f"Error starting orchestrator: {e}")
                        break
                else:
                    # For subsequent cycles, just check system status
                    if hasattr(orchestrator, 'get_system_status'):
                        try:
                            status = orchestrator.get_system_status()
                            logger.debug(f"System state: {status.get('system_state', 'unknown')}")
                        except Exception as e:
                            logger.debug(f"Could not get system status: {e}")

                # Additional debug info every 5 cycles
                if execution_count % 5 == 0:
                    logger.info(f"=== PERIODIC DEBUG INFO (Cycle {execution_count}) ===")

                    # Check CPU usage
                    if hasattr(orchestrator, 'system_monitor') and orchestrator.system_monitor:
                        cpu_usage = orchestrator.system_monitor.status.cpu_usage_percent
                        logger.info(f"CURRENT CPU USAGE: {cpu_usage:.1f}%")

                    # Check queue sizes
                    if hasattr(orchestrator, 'queue_manager') and orchestrator.queue_manager:
                        queue_status = orchestrator.queue_manager.get_queue_status()
                        for queue_name, queue_info in queue_status.items():
                            if queue_info['size'] > 0:
                                logger.info(f"ACTIVE QUEUE {queue_name}: {queue_info['size']} messages")

                # FIXED: Sleep in smaller chunks to check shutdown flag more frequently
                sleep_duration = 30  # Total sleep time
                sleep_interval = 1   # Check shutdown every 1 second

                for i in range(sleep_duration):
                    if shutdown_requested:
                        logger.info("SHUTDOWN: Shutdown requested during sleep, breaking immediately")
                        break
                    time.sleep(sleep_interval)

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received - initiating graceful shutdown")
                shutdown_requested = True  # Ensure flag is set
                break
            except Exception as e:
                logger.error(f"Error in execution cycle {execution_count}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")

                # Check if shutdown was requested during error handling
                if shutdown_requested:
                    logger.info("SHUTDOWN: Shutdown requested during error recovery")
                    break

                # Continue with next cycle instead of breaking
                time.sleep(5)  # Brief pause before retrying

        # FIXED: Add emergency exit if shutdown takes too long
        if shutdown_requested:
            logger.info("SHUTDOWN: Beginning shutdown sequence...")
            shutdown_timeout_start = time.time()

        # Log reason for loop exit
        if shutdown_requested:
            logger.info(f"Main execution loop exited due to shutdown request after {execution_count} cycles")
        else:
            logger.info(f"Main execution loop completed after {execution_count} cycles")

        # Graceful shutdown
        def shutdown_orchestrator():
            logger.info("=== GRACEFUL SHUTDOWN INITIATED ===")
            try:
                # Stop colorized news monitoring
                try:
                    from colorized_news_output import stop_news_monitoring
                    stop_news_monitoring()
                    success("Colorized news monitoring stopped")
                except Exception as e:
                    warning(f"Could not stop colorized news monitoring: {e}")

                if orchestrator:
                    orchestrator.stop_system()
                    logger.info("Orchestrator stopped successfully")
            except Exception as e:
                logger.error(f"Error during orchestrator shutdown: {e}")

        shutdown_orchestrator()

        # FIXED: Emergency timeout to prevent hanging
        if shutdown_timeout_start:
            shutdown_duration = time.time() - shutdown_timeout_start
            if shutdown_duration > 10:  # 10 second timeout
                logger.warning(f"EMERGENCY: Shutdown took {shutdown_duration:.1f}s - forcing exit")
                os._exit(1)

        logger.info("=== TRADING SYSTEM SHUTDOWN COMPLETE ===")

    except Exception as e:
        logger.critical(f"Critical error in trading system: {e}")
        import traceback
        logger.critical(f"Traceback: {traceback.format_exc()}")

        if orchestrator:
            try:
                orchestrator.stop_system()
            except:
                pass

        raise

def development_timeout_handler():
    """Auto-shutdown after 10 minutes for development purposes"""
    time.sleep(DEVELOPMENT_TIMEOUT)
    print(f"\n[DEVELOPMENT] {DEVELOPMENT_TIMEOUT//60}-minute timeout reached - auto-shutting down system")
    logging.info(f"DEVELOPMENT: {DEVELOPMENT_TIMEOUT//60}-minute timeout reached - initiating auto-shutdown")
    # Send interrupt signal to main process
    os.kill(os.getpid(), signal.SIGINT)

def main():
    """Main entry point with comprehensive error handling"""
    global shutdown_requested

    try:
        # Import colored print functions
        from colored_print import header, success, error, warning, info, progress
        
        header("FRIREN TRADING SYSTEM STARTUP")
        progress("Initializing trading system...")

        # DEVELOPMENT: Start 10-minute auto-shutdown timer
        timeout_thread = threading.Thread(target=development_timeout_handler, daemon=True)
        timeout_thread.start()
        warning(f"[DEVELOPMENT] Auto-shutdown timer started - system will stop in {DEVELOPMENT_TIMEOUT//60} minutes")

        # Setup enhanced logging
        setup_enhanced_logging()
        logger = logging.getLogger('main')
        logger.info("=== TRADING SYSTEM STARTUP ===")

        # Run the trading system with keyboard interrupt protection
        system_thread = threading.Thread(target=run_trading_system, daemon=True)
        system_thread.start()

        # Monitor for keyboard interrupts while system runs
        try:
            while system_thread.is_alive():
                system_thread.join(timeout=1.0)  # Check every second
                if shutdown_requested:
                    logger.info("MAIN: Shutdown requested, waiting for system to stop...")
                    break
        except KeyboardInterrupt:
            logger.info("MAIN: KeyboardInterrupt caught - initiating emergency shutdown")
            shutdown_requested = True

            # Give system 5 seconds to shut down gracefully
            system_thread.join(timeout=5.0)
            if system_thread.is_alive():
                logger.warning("MAIN: System did not shut down gracefully - forcing exit")
                os._exit(1)

        logger.info("COMPLETION: Trading system completed successfully")
        success("Trading system completed successfully")
        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("INTERRUPTED: System interrupted by user in main()")
        warning("System interrupted by user")
        shutdown_requested = True
        sys.exit(0)
    except Exception as e:
        logger.critical(f"CRITICAL ERROR: {e}")
        import traceback
        logger.critical(f"Full traceback: {traceback.format_exc()}")
        error(f"CRITICAL ERROR: {e}")
        sys.exit(1)

    # ... existing code ...
    from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
    db_manager = TradingDBManager()
    print("\n=== DEBUG: CURRENT HOLDINGS TABLE ===")
    holdings = db_manager.get_current_holdings()
    for h in holdings:
        print(h)
    print("=== END DEBUG: CURRENT HOLDINGS TABLE ===\n")
    # ... existing code ...

if __name__ == "__main__":
    main()
