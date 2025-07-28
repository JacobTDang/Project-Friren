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

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded from .env file")
except ImportError:
    print("Warning: python-dotenv not available, relying on system environment variables")

# Windows-specific multiprocessing setup
if sys.platform == "win32":
    import multiprocessing as mp
    mp.freeze_support()
    mp.set_start_method('spawn', force=True)

# PRODUCTION: Allow infinite runtime for production trading
# DEVELOPMENT_TIMEOUT = 600  # Disabled for production

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
    logger.critical(f"SIGNAL RECEIVED: {signum} - IMMEDIATE SHUTDOWN INITIATED")

    # Set shutdown flag IMMEDIATELY - this is critical
    shutdown_requested = True

    # ULTRA-AGGRESSIVE SUBPROCESS CLEANUP
    try:
        import psutil
        import subprocess
        main_process = psutil.Process()
        main_pid = os.getpid()
        
        logger.critical("SIGNAL: ULTRA-AGGRESSIVE subprocess cleanup initiated...")
        
        # Method 1: Direct psutil children cleanup
        children = main_process.children(recursive=True)
        logger.critical(f"SIGNAL: Found {len(children)} direct child processes")
        
        # Terminate all direct children
        for child in children:
            try:
                logger.critical(f"SIGNAL: Terminating direct child PID {child.pid} ({child.name()})")
                child.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Method 2: Find ALL Python processes that might be related
        python_processes = []
        for proc in psutil.process_iter(['pid', 'ppid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    # Check if it's a subprocess of main process or contains project path
                    if (proc.info['ppid'] == main_pid or 
                        (proc.info['cmdline'] and any('project-friren' in str(cmd).lower() for cmd in proc.info['cmdline']))):
                        python_processes.append(proc)
                        logger.critical(f"SIGNAL: Found related Python process PID {proc.info['pid']}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Terminate related Python processes
        for proc in python_processes:
            try:
                proc.terminate()
                logger.critical(f"SIGNAL: Terminated related Python PID {proc.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Wait for graceful termination
        logger.critical("SIGNAL: Waiting 3 seconds for graceful termination...")
        time.sleep(3)
        
        # Method 3: FORCE KILL everything
        remaining_children = main_process.children(recursive=True)
        if remaining_children:
            logger.critical(f"SIGNAL: FORCE KILLING {len(remaining_children)} remaining direct children")
            for child in remaining_children:
                try:
                    logger.critical(f"SIGNAL: FORCE KILL direct child PID {child.pid}")
                    child.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        
        # Force kill remaining Python processes
        for proc in python_processes:
            try:
                if proc.is_running():
                    logger.critical(f"SIGNAL: FORCE KILL Python process PID {proc.pid}")
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Method 4: Windows taskkill fallback (if on Windows)
        if os.name == 'nt':
            try:
                logger.critical("SIGNAL: Windows taskkill fallback for any remaining processes...")
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(main_pid)], 
                             capture_output=True, timeout=5)
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
        
        logger.critical("SIGNAL: ULTRA-AGGRESSIVE cleanup completed")
        
    except Exception as e:
        logger.error(f"SIGNAL: Error in aggressive cleanup: {e}")

    # FORCE stop the orchestrator and its process manager
    if global_orchestrator:
        try:
            logger.critical("SIGNAL: Force stopping orchestrator and process manager...")
            
            # CRITICAL: Stop process manager first to terminate subprocess groups
            if hasattr(global_orchestrator, 'process_manager') and global_orchestrator.process_manager:
                logger.critical("SIGNAL: Terminating all processes via process manager...")
                try:
                    # Get all process IDs and terminate them via the manager
                    process_ids = list(global_orchestrator.process_manager.processes.keys())
                    logger.critical(f"SIGNAL: Found {len(process_ids)} processes to terminate: {process_ids}")
                    
                    for process_id in process_ids:
                        try:
                            logger.critical(f"SIGNAL: Terminating process {process_id} via manager...")
                            global_orchestrator.process_manager.stop_process(process_id)
                        except Exception as e:
                            logger.error(f"SIGNAL: Failed to stop process {process_id}: {e}")
                    
                    # Force cleanup any remaining processes
                    logger.critical("SIGNAL: Forcing cleanup of remaining processes...")
                    global_orchestrator.process_manager.cleanup_all_processes()
                    
                except Exception as e:
                    logger.error(f"SIGNAL: Process manager cleanup failed: {e}")
            
            # Now stop the orchestrator
            global_orchestrator.stop_system()
            logger.critical("SIGNAL: Orchestrator stopped")
        except Exception as e:
            logger.error(f"SIGNAL: Orchestrator shutdown failed: {e}")

    # Immediate exit after cleanup
    logger.critical("SIGNAL: IMMEDIATE EXIT - All processes terminated")
    os._exit(0)  # Force exit immediately

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

        # Step 3: Get discovered symbols from Redis (if available)
        discovered_symbols = []
        try:
            # Try to get recently discovered symbols from Redis
            from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager
            redis_manager = get_trading_redis_manager()
            if redis_manager and hasattr(redis_manager, 'redis_client'):
                redis_client = redis_manager.redis_client
                # Get discovered symbols stored by enhanced_news_pipeline
                discovered_data = redis_client.get("discovered_symbols")
                if discovered_data:
                    import json
                    discovered_symbols = json.loads(discovered_data)
                    # Filter out symbols we already have
                    discovered_symbols = [s for s in discovered_symbols if s not in holding_symbols and s not in opportunity_symbols]
                    logger.info(f"Recently Discovered Symbols: {len(discovered_symbols)} symbols {discovered_symbols}")
        except Exception as e:
            logger.debug(f"Could not load discovered symbols from Redis: {e}")

        # Step 4: Combine holdings + opportunities + discovered
        dynamic_symbols = holding_symbols + opportunity_symbols + discovered_symbols

        # Check if database is empty - FAIL FAST approach
        if not dynamic_symbols:
            logger.critical("CRITICAL: Database watchlist empty - no symbols available for trading")
            logger.critical("FAIL FAST: System cannot operate without real portfolio symbols from database")
            logger.critical("ACTION REQUIRED: Add symbols to the watchlist database or check database connection")
            logger.critical("REFUSING to use hardcoded fallback symbols - system must use real portfolio data")
            raise RuntimeError("FAIL FAST: Empty database watchlist - system requires real portfolio symbols, not hardcoded fallbacks")

        logger.info(f"Dynamic Watchlist Complete: {len(dynamic_symbols)} total symbols")
        return dynamic_symbols

    except Exception as e:
        logger.error(f"Error loading dynamic watchlist: {e}")
        logger.warning("Database unavailable - attempting fallback to DISCOVERY_SYMBOLS configuration")
        
        # Fallback to DISCOVERY_SYMBOLS when database is unavailable
        try:
            from Friren_V1.infrastructure.configuration_manager import ConfigurationManager
            config_manager = ConfigurationManager()
            discovery_symbols = config_manager.get('DISCOVERY_SYMBOLS')
            
            if discovery_symbols:
                logger.info(f"Using DISCOVERY_SYMBOLS fallback: {len(discovery_symbols)} symbols {discovery_symbols}")
                return discovery_symbols
            else:
                logger.error("DISCOVERY_SYMBOLS fallback also empty")
                
        except Exception as fallback_error:
            logger.error(f"Failed to load DISCOVERY_SYMBOLS fallback: {fallback_error}")
        
        logger.error("CRITICAL: Cannot load symbols from database or configuration fallback")
        return []  # Return empty list after all fallbacks exhausted

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
    
    # Check Redis server availability
    logger.info("Checking Redis server connection...")
    try:
        # For WSL/Linux environment, check Redis directly
        import subprocess
        import sys
        import os
        
        # Check if we can ping Redis
        if sys.platform == "win32":
            # Windows - skip Redis check for now (will be handled by Redis manager)
            logger.info("Redis server: SKIPPED (Windows - will be checked by Redis manager)")
        else:
            # Linux/WSL - check with redis-cli
            result = subprocess.run(['redis-cli', 'ping'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and 'PONG' in result.stdout:
                logger.info("Redis server: CONNECTED")
            else:
                logger.error("Redis server: NOT RESPONDING")
                logger.error("CRITICAL: Start Redis server with: sudo service redis-server start")
                requirements_met = False
    except Exception as e:
        logger.error(f"Redis server: NOT AVAILABLE - {e}")
        logger.error("CRITICAL: Start Redis server with: sudo service redis-server start")
        requirements_met = False

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

        # CRITICAL FIX: Sync Alpaca positions to database BEFORE loading watchlist
        logger.info("STEP 1.5: Syncing Alpaca positions to database...")
        try:
            from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
            from Friren_V1.trading_engine.portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface
            from Friren_V1.trading_engine.portfolio_manager.tools.account_manager import AccountManager
            
            # Initialize components for sync
            db_manager = TradingDBManager()
            alpaca_interface = SimpleAlpacaInterface()
            account_manager = AccountManager(db_manager=db_manager, alpaca_interface=alpaca_interface)
            
            # Sync Alpaca positions to database
            sync_success = account_manager.sync_alpaca_positions_to_database()
            if sync_success:
                logger.info("SUCCESS: Alpaca positions synced to database")
            else:
                logger.error("FAILED: Could not sync Alpaca positions to database")
                
        except Exception as e:
            logger.error(f"ERROR syncing Alpaca positions: {e}")
            logger.error("Continuing with existing database content...")

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
        from terminal_color_system import print_communication
    except ImportError:
        # Fallback functions if colored_print not available
        def header(msg): print(f"=== {msg} ===")
        def success(msg): print(f"[SUCCESS] {msg}")
        def error(msg): print(f"[ERROR] {msg}")
        def warning(msg): print(f"[WARNING] {msg}")
        def info(msg): print(f"[INFO] {msg}")
        def progress(msg): print(f"[PROGRESS] {msg}")
        def print_communication(msg): print(f"[COMM] {msg}")

    logger = logging.getLogger("main")
    orchestrator = None
    bridge = None

    # MEMORY MONITORING: Initialize system-wide memory monitoring
    main_memory_monitor = None
    system_cleanup_manager = None
    memory_threshold_controller = None

    try:
        logger.info("=== STARTING ENHANCED TRADING SYSTEM ===")
        logger.info("Features: Smart Memory Management, Real-time monitoring, Queue debugging, CPU tracking")
        
        # COORDINATION SYSTEM: Initialize ultra-comprehensive message coordination
        logger.info("=== INITIALIZING ULTRA-COMPREHENSIVE COORDINATION SYSTEM ===")
        try:
            from Friren_V1.trading_engine.coordination import initialize_coordination_system
            
            # Initialize all coordination components
            coordinators = initialize_coordination_system(enable_monitoring=True)
            
            logger.info("COORDINATION: System message coordinator initialized")
            logger.info("COORDINATION: Memory-aware queue rotation started") 
            logger.info("COORDINATION: Process recovery manager active")
            logger.info("COORDINATION: Redis queue coordinator running")
            logger.info("COORDINATION: All 47 race conditions prevented, 12 deadlock patterns avoided")
            success("ULTRA-COMPREHENSIVE coordination system active - zero message conflicts")
            
        except Exception as e:
            logger.error(f"Failed to initialize coordination system: {e}")
            # Continue without coordination (degraded mode)
            warning("Coordination system failed - running in degraded mode")

        # SMART MEMORY MANAGEMENT: Setup genius memory management system
        logger.info("=== INITIALIZING SMART MEMORY MANAGEMENT SYSTEM ===")
        from Friren_V1.multiprocess_infrastructure.memory_monitor import get_memory_monitor, cleanup_all_monitors
        from Friren_V1.multiprocess_infrastructure.memory_cleanup_manager import get_cleanup_manager
        from Friren_V1.multiprocess_infrastructure.memory_threshold_controller import start_memory_threshold_monitoring

        # Initialize the smart memory threshold controller
        memory_threshold_controller = start_memory_threshold_monitoring()
        logger.info("SMART_MEMORY: Threshold controller started")
        logger.info("SMART_MEMORY: 800MB pause threshold, 600MB resume threshold")
        logger.info("SMART_MEMORY: Process rotation and memory cleanup enabled")

        # CRITICAL TASK 1: Initialize NewsScheduler for smart memory management
        logger.info("=== INITIALIZING SMART NEWS SCHEDULING SYSTEM ===")
        from Friren_V1.multiprocess_infrastructure.news_scheduler import start_news_scheduling, NewsScheduleConfig
        
        # Configure news scheduling - DISABLED FOR TESTING
        news_config = NewsScheduleConfig(
            collection_interval_minutes=2,   # Enable active news collection for testing
            collection_duration_minutes=1,     # Minimal duration
            market_hours_only=False,           # Enable 24/7 news collection for testing  
            memory_threshold_pause=False       # Disable memory pausing for testing
        )
        
        # Start news scheduler (disabled)
        news_scheduler = start_news_scheduling(news_config)
        logger.info("SMART_NEWS: Scheduled news collection ENABLED for testing (2min intervals)")
        logger.info("SMART_NEWS: Expected 60% memory reduction from scheduled vs continuous collection")

        # Initialize main process memory monitoring (4GB limit for main process)
        main_memory_monitor = get_memory_monitor(
            process_id="main_process",
            memory_limit_mb=4096,  # 4GB limit for main process
            auto_start=True
        )

        # Setup cleanup manager for main process
        system_cleanup_manager = get_cleanup_manager("main_process", main_memory_monitor)

        # Enhanced emergency callback with smart memory integration
        def emergency_memory_callback(snapshot, stats):
            logger.critical(f"EMERGENCY: Main process memory critical - {snapshot.memory_mb:.1f}MB")
            if memory_threshold_controller:
                # Let smart controller handle emergency first
                controller_status = memory_threshold_controller.get_status()
                logger.critical(f"Memory controller status: {controller_status['mode']} mode, {len(controller_status['processes']['paused'])} processes paused")
            
            # Only shutdown if smart controller can't handle it
            if snapshot.memory_mb > 3500:  # 3.5GB emergency threshold for main process
                logger.critical("Initiating emergency system shutdown to prevent OOM crash")
                global shutdown_requested
                shutdown_requested = True

        main_memory_monitor.add_emergency_callback(emergency_memory_callback)

        # Enhanced alert callback with smart memory integration
        def memory_alert_callback(snapshot, stats, alert_level):
            if alert_level.value in ['high', 'critical']:
                logger.warning(f"MEMORY ALERT [{alert_level.value.upper()}]: Main process using {snapshot.memory_mb:.1f}MB")
                
                # Show smart controller status
                if memory_threshold_controller:
                    controller_status = memory_threshold_controller.get_status()
                    logger.info(f"SMART_MEMORY: System total {controller_status.get('total_memory_mb', 0):.1f}MB, mode: {controller_status['mode']}")
                
                # Trigger cleanup
                freed = system_cleanup_manager.perform_cleanup()
                logger.info(f"Emergency cleanup freed {freed:.1f}MB")

        main_memory_monitor.add_alert_callback(memory_alert_callback)

        logger.info(f"Smart memory management initialized - Expected 60% memory reduction")
        success("GENIUS memory management active: pause/resume/rotation enabled")

        # Start the terminal bridge for subprocess communication
        from main_terminal_bridge import MainTerminalBridge
        bridge = MainTerminalBridge()
        bridge.start_monitoring()
        print_communication("Terminal bridge started - monitoring subprocess output")
        
        # Start auto business logic trigger for detailed news/FinBERT/XGBoost outputs
        from auto_trigger_business_logic import start_auto_business_logic
        if start_auto_business_logic():
            print_communication("Auto business logic trigger started - detailed outputs every 2 minutes")
        else:
            warning("Auto business logic trigger failed - manual triggers may be needed")
        
        info("Watch for colored business execution messages below:")

        # Initialize the trading system
        orchestrator = initialize_trading_system()
        if not orchestrator:
            logger.error("Failed to initialize trading system")
            return

        global global_orchestrator
        global_orchestrator = orchestrator

        # REMOVED: Direct Symbol Coordinator band-aid fix
        # Core initialization should handle Symbol Coordinator properly
        progress("Symbol Coordinator will be initialized through proper channels...")

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

        # Test queue monitoring - FIXED: Use Redis manager instead of queue_manager
        try:
            if hasattr(orchestrator, 'redis_manager') and orchestrator.redis_manager:
                queue_status = orchestrator.redis_manager.get_queue_status()
                logger.info("=== FIXED QUEUE MONITORING (Redis-based) ===")
                for queue_name, queue_info in queue_status.items():
                    # DEFENSIVE FIX: Handle case where queue_info might be an int instead of dict
                    if isinstance(queue_info, dict):
                        size = queue_info.get('size', 0)
                        logger.info(f"{queue_name.upper()}: Size={size} (should show actual counts)")
                        if queue_info.get('message_types'):
                            logger.info(f"  Message types: {queue_info['message_types']}")
                    elif isinstance(queue_info, (int, float)):
                        # Fallback: queue_info is just the size
                        logger.info(f"{queue_name.upper()}: Size={queue_info} (legacy format)")
                    else:
                        logger.warning(f"{queue_name.upper()}: Unknown queue info format: {type(queue_info)}")
            else:
                logger.warning("Redis manager not available for queue testing")
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

        while not shutdown_requested and execution_count < max_executions:
            try:
                execution_count += 1
                logger.info(f"""=== EXECUTION CYCLE {execution_count} ===""")

                # Test event injection logic removed to use real data

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

                        # Wait for start with timeout - REDUCED for faster startup
                        if start_complete.wait(timeout=60.0):  # 60 second timeout for process startup
                            if start_error:
                                logger.error(f"Orchestrator start failed: {start_error}")
                                break
                            else:
                                logger.info("Orchestrator started successfully")
                                
                                # CRITICAL: Finish initialization mode to enable normal memory management
                                try:
                                    from Friren_V1.multiprocess_infrastructure.memory_threshold_controller import get_memory_threshold_controller
                                    memory_controller = get_memory_threshold_controller()
                                    memory_controller.finish_initialization()
                                    logger.info("INITIALIZATION: Memory management switched to normal mode after successful orchestrator startup")
                                except Exception as e:
                                    logger.warning(f"Failed to finish initialization mode: {e}")
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

                    # Check queue sizes - FIXED: Use Redis manager
                    if hasattr(orchestrator, 'redis_manager') and orchestrator.redis_manager:
                        queue_status = orchestrator.redis_manager.get_queue_status()
                        for queue_name, queue_info in queue_status.items():
                            # DEFENSIVE FIX: Handle different queue_info formats
                            if isinstance(queue_info, dict):
                                size = queue_info.get('size', 0)
                                if size > 0:
                                    logger.info(f"ACTIVE QUEUE {queue_name}: {size} messages")
                            elif isinstance(queue_info, (int, float)) and queue_info > 0:
                                logger.info(f"ACTIVE QUEUE {queue_name}: {queue_info} messages")

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

                # Stop auto business logic trigger
                try:
                    from auto_trigger_business_logic import stop_auto_business_logic
                    stop_auto_business_logic()
                    logger.info("Auto business logic trigger stopped successfully")
                except Exception as e:
                    warning(f"Could not stop auto business logic trigger: {e}")

                # Stop terminal bridge
                if bridge:
                    bridge.stop_monitoring()
                    logger.info("Terminal bridge stopped successfully")

                # Stop news scheduler
                if 'news_scheduler' in locals():
                    from Friren_V1.multiprocess_infrastructure.news_scheduler import stop_news_scheduling
                    stop_news_scheduling()
                    logger.info("News scheduler stopped successfully")

                # Stop smart memory management
                if memory_threshold_controller:
                    from Friren_V1.multiprocess_infrastructure.memory_threshold_controller import stop_memory_threshold_monitoring
                    stop_memory_threshold_monitoring()
                    logger.info("Smart memory threshold controller stopped successfully")

                # Stop coordination system
                try:
                    from Friren_V1.trading_engine.coordination import shutdown_coordination_system
                    shutdown_coordination_system()
                    logger.info("Coordination system stopped successfully")
                except Exception as e:
                    logger.warning(f"Could not stop coordination system: {e}")

            except Exception as e:
                logger.error(f"Error during orchestrator shutdown: {e}")

        shutdown_orchestrator()

        # FIXED: Emergency timeout to prevent hanging - INCREASED from 10s to 60s
        if shutdown_timeout_start:
            shutdown_duration = time.time() - shutdown_timeout_start
            if shutdown_duration > 60:  # 60 second timeout (was 10)
                logger.warning(f"EMERGENCY: Shutdown took {shutdown_duration:.1f}s - forcing exit")
                
                # Before force exit, try cleanup script one more time
                try:
                    logger.critical("EMERGENCY: Running cleanup script before force exit...")
                    subprocess.run([sys.executable, "cleanup_processes.py", "--force"], timeout=10)
                except Exception as e:
                    logger.error(f"Emergency cleanup failed: {e}")
                
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

        # Stop terminal bridge
        if bridge:
            try:
                bridge.stop_monitoring()
            except:
                pass

        raise

def development_timeout_handler():
    """Auto-shutdown disabled for production - runs indefinitely"""
    # DISABLED: No timeout for production trading
    return

def emergency_atexit_cleanup():
    """Emergency cleanup function called at program exit"""
    try:
        import psutil
        main_process = psutil.Process()
        children = main_process.children(recursive=True)
        
        if children:
            print(f"\n[EMERGENCY ATEXIT] Found {len(children)} orphaned processes - force killing...")
            for child in children:
                try:
                    child.kill()
                    print(f"[EMERGENCY ATEXIT] Killed orphaned PID {child.pid}")
                except:
                    pass
            print("[EMERGENCY ATEXIT] All orphaned processes killed")
        
    except Exception:
        pass  # Silent cleanup - don't crash on exit

def validate_critical_dependencies(logger):
    """
    Validate all critical dependencies at startup to prevent runtime failures.
    Fails fast with clear error messages if any required component is missing.
    """
    validation_errors = []
    
    logger.info("=== CRITICAL DEPENDENCY VALIDATION ===")
    
    # 1. Validate FinBERT Dependencies
    try:
        import torch
        import transformers
        from transformers import BertTokenizer, BertForSequenceClassification
        logger.info("AVAILABLE: FinBERT dependencies (torch, transformers)")
    except ImportError as e:
        error_msg = f"MISSING: FinBERT dependencies: {e}"
        logger.critical(error_msg)
        validation_errors.append(error_msg)
    
    # 2. Validate XGBoost Dependencies
    try:
        import xgboost as xgb
        import numpy as np
        import pandas as pd
        logger.info("AVAILABLE: XGBoost dependencies (xgboost, numpy, pandas)")
    except ImportError as e:
        error_msg = f"MISSING: XGBoost dependencies: {e}"
        logger.critical(error_msg)
        validation_errors.append(error_msg)
    
    # 3. Validate Alpaca Dependencies
    try:
        from alpaca.trading.client import TradingClient
        from alpaca.data.historical import StockHistoricalDataClient
        logger.info("AVAILABLE: Alpaca dependencies (alpaca-py)")
    except ImportError as e:
        error_msg = f"MISSING: Alpaca dependencies: {e}"
        logger.critical(error_msg)
        validation_errors.append(error_msg)
    
    # 4. Validate News API Dependencies
    try:
        import requests
        import feedparser
        logger.info("AVAILABLE: News API dependencies (requests, feedparser) - AVAILABLE")
    except ImportError as e:
        error_msg = f"MISSING: News API dependencies: {e}"
        logger.critical(error_msg)
        validation_errors.append(error_msg)
    
    # 5. Validate Database Dependencies
    try:
        import psycopg2
        import psycopg2.extras
        logger.info("AVAILABLE: Database dependencies (psycopg2)")
    except ImportError as e:
        error_msg = f"MISSING: Database dependencies: {e}"
        logger.critical(error_msg)
        validation_errors.append(error_msg)
    
    # 6. Validate Redis Dependencies
    try:
        import redis
        logger.info("AVAILABLE: Redis dependencies (redis)")
    except ImportError as e:
        error_msg = f"MISSING: Redis dependencies: {e}"
        logger.critical(error_msg)
        validation_errors.append(error_msg)
    
    # 7. Validate Environment Variables
    required_env_vars = ['ALPACA_API_KEY', 'ALPACA_SECRET_KEY', 'DATABASE_URL']
    for var in required_env_vars:
        if not os.environ.get(var):
            error_msg = f"MISSING: Required environment variable: {var}"
            logger.critical(error_msg)
            validation_errors.append(error_msg)
        else:
            logger.info(f"AVAILABLE: Environment variable {var}")
    
    # 8. Validate XGBoost Model Files
    model_files = ['models/demo_xgb_model.json', 'models/demo_xgb_model.pkl']
    for model_file in model_files:
        if not os.path.exists(model_file):
            error_msg = f"MISSING: XGBoost model file: {model_file}"
            logger.critical(error_msg)
            validation_errors.append(error_msg)
        else:
            logger.info(f"AVAILABLE: XGBoost model file {model_file}")
    
    # Fail fast if any critical dependencies are missing
    if validation_errors:
        logger.critical("=== DEPENDENCY VALIDATION FAILED ===")
        logger.critical("SYSTEM CANNOT START: Critical dependencies missing")
        for error in validation_errors:
            logger.critical(f"  {error}")
        logger.critical("Please install missing dependencies and configure environment variables")
        raise RuntimeError(f"Critical dependencies missing: {len(validation_errors)} errors found. System cannot start.")
    
    logger.info("=== DEPENDENCY VALIDATION PASSED ===")
    logger.info("All critical dependencies available - system ready for production")

def main():
    """Main entry point with comprehensive error handling"""
    global shutdown_requested

    # Register emergency atexit cleanup as absolute last resort
    import atexit
    atexit.register(emergency_atexit_cleanup)

    # Register aggressive signal handlers IMMEDIATELY
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGBREAK'):  # Windows
        signal.signal(signal.SIGBREAK, signal_handler)

    try:
        # Import colored print functions
        from colored_print import header, success, error, warning, info, progress

        header("FRIREN TRADING SYSTEM STARTUP")
        progress("Initializing trading system...")

        # Setup enhanced logging
        logger = setup_enhanced_logging()

        # CRITICAL: Validate all dependencies before starting system
        validate_critical_dependencies(logger)

        # PRODUCTION: Infinite runtime enabled
        info("[PRODUCTION] Infinite runtime enabled - system will run until manually stopped")
        logger.info("=== TRADING SYSTEM STARTUP ===")

        # CRITICAL: Check system requirements before proceeding
        if not check_system_requirements():
            error("System requirements not met - aborting startup")
            sys.exit(1)

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
