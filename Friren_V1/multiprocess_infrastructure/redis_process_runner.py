"""
Redis process runner script

This script is launched as a subprocess and runs individual trading system
processes using Redis for communication instead of multiprocessing queues.
"""

import sys
import os
import json
import logging
import importlib
import time
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

def setup_logging(process_id: str):
    """Setup logging for the spawned process"""
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(project_root, 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    # Setup logging
    log_file = os.path.join(logs_dir, f'{process_id}_redis_subprocess.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(f"redis_subprocess.{process_id}")
    return logger

def load_process_class(module_path: str, class_name: str):
    """Dynamically load a process class"""
    try:
        module = importlib.import_module(module_path)
        process_class = getattr(module, class_name)
        return process_class
    except Exception as e:
        raise ImportError(f"Could not load class {class_name} from {module_path}: {e}")

def create_redis_compatible_process(original_class, process_id: str, process_args: dict, heartbeat_interval: int):
    """Create a Redis-compatible wrapper for existing process classes"""
    from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
    from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager

    class RedisCompatibleProcess(RedisBaseProcess):
        """Redis-compatible wrapper for existing process classes"""

        def __init__(self, process_id: str, heartbeat_interval: int = 30, **kwargs):
            # Initialize parent with memory limit
            import os
            default_memory = int(os.getenv('FRIREN_BASE_PROCESS_MEMORY_MB', '400'))
            memory_limit_mb = kwargs.get('memory_limit_mb', default_memory)
            super().__init__(process_id, heartbeat_interval, memory_limit_mb=memory_limit_mb)

            # Store original class and args
            self.original_class = original_class
            self.process_args = kwargs
            self.logger.info(f"Using business logic from: {original_class.__name__}")

            # Initialize business logic
            try:
                self.business_class = original_class
                self._import_business_methods()
                self._initialize_business_attributes()
                self.logger.info(f"Successfully integrated business logic from {original_class.__name__}")

            except Exception as e:
                self.logger.error(f"Failed to integrate business logic: {e}")
                self._initialize_business_attributes()

        def _create_minimal_business_instance(self, business_args):
            """Create business instance with minimal initialization"""
            try:
                # SPECIFIC FIX #2: Remove auto_start parameter properly - EnhancedMarketDecisionEngineProcess doesn't accept it
                cleaned_args = business_args.copy()
                if 'auto_start' in cleaned_args:
                    cleaned_args.pop('auto_start')
                    self.logger.debug(f"Removed auto_start parameter for {self.business_class.__name__}")
                
                # Create instance
                instance = self.business_class(**cleaned_args)
                
                # Mark for full initialization in start()
                instance._minimal_init = True
                
                return instance
                
            except Exception as e:
                self.logger.error(f"Minimal business instance creation failed: {e}")
                raise

        def _import_business_methods(self):
            """Import business methods from the original class dynamically"""
            try:
                imported_methods = []
                
                # Create business instance to get working methods
                try:
                    import inspect
                    sig = inspect.signature(self.business_class.__init__)
                    
                    # Prepare arguments for business class constructor
                    business_args = {'process_id': self.process_id}
                    
                    # Add heartbeat_interval if expected
                    if 'heartbeat_interval' in sig.parameters:
                        business_args['heartbeat_interval'] = getattr(self, 'heartbeat_interval', 30)
                    
                    # Add process args that match constructor parameters
                    for param_name in sig.parameters:
                        if param_name in self.process_args:
                            business_args[param_name] = self.process_args[param_name]
                    
                    # Create business instance
                    business_instance = self._create_minimal_business_instance(business_args)
                    
                    # Store reference to business instance
                    self._business_instance = business_instance
                    
                    # Synchronize attributes
                    self._synchronize_attributes_to_business_instance()
                    
                    # Extract and bind key methods
                    if hasattr(business_instance, '_process_cycle'):
                        def wrapped_process_cycle():
                            return self._business_instance._process_cycle()
                        self._business_process_cycle = wrapped_process_cycle
                        imported_methods.append('_business_process_cycle')
                        self.logger.info("Successfully bound _process_cycle as _business_process_cycle")
                    
                    if hasattr(business_instance, '_initialize'):
                        def wrapped_initialize():
                            return self._business_instance._initialize()
                        self._business_initialize = wrapped_initialize
                        imported_methods.append('_business_initialize')
                        self.logger.info("Successfully bound _initialize as _business_initialize")
                    
                    # Bind discovery scan method for enhanced_news_pipeline
                    if hasattr(business_instance, '_run_stock_discovery_scan'):
                        def wrapped_discovery_scan():
                            return self._business_instance._run_stock_discovery_scan()
                        self._run_stock_discovery_scan = wrapped_discovery_scan
                        imported_methods.append('_run_stock_discovery_scan')
                        self.logger.info("Successfully bound _run_stock_discovery_scan method")
                    
                    # Copy important attributes
                    for attr in ['symbols', 'watchlist_symbols', 'confidence_threshold']:
                        if hasattr(business_instance, attr):
                            setattr(self, attr, getattr(business_instance, attr))
                            imported_methods.append(f"attr:{attr}")
                    
                    self.logger.info(f"Successfully created and bound business instance: {type(business_instance).__name__}")
                    
                except Exception as e:
                    self.logger.warning(f"Could not create business instance, falling back to method copying: {e}")
                    
                    # Fallback: Copy methods from the business class
                    for attr_name in dir(self.business_class):
                        if attr_name.startswith('_') and callable(getattr(self.business_class, attr_name)):
                            try:
                                # Skip special/magic methods 
                                if attr_name.startswith('__'):
                                    continue
                                    
                                # Get the unbound method and bind it to self
                                unbound_method = getattr(self.business_class, attr_name)
                                if hasattr(unbound_method, '__get__'):
                                    # Bind the method to this wrapper instance
                                    bound_method = unbound_method.__get__(self, self.__class__)
                                    setattr(self, attr_name, bound_method)
                                    imported_methods.append(attr_name)
                                    
                                    # Special handling for key methods
                                    if attr_name == '_process_cycle':
                                        self._business_process_cycle = bound_method
                                    elif attr_name == '_initialize':
                                        self._business_initialize = bound_method
                                    
                            except Exception as e:
                                self.logger.debug(f"Could not bind method {attr_name}: {e}")
                                continue

                self.logger.info(f"Imported {len(imported_methods)} business methods: {imported_methods[:10]}{'...' if len(imported_methods) > 10 else ''}")
                
                # Verify critical methods are available
                if hasattr(self, '_business_process_cycle'):
                    self.logger.info(f"Business process cycle method available for {self.business_class.__name__}")
                elif hasattr(self, '_process_cycle'):
                    self.logger.info(f"Legacy _process_cycle method available for {self.business_class.__name__}")
                else:
                    self.logger.warning(f"No process cycle method found for {self.business_class.__name__}")

            except Exception as e:
                self.logger.error(f"Failed to import business methods: {e}")

        def _initialize_business_attributes(self):
            """Initialize attributes needed by business logic"""
            try:
                # Initialize common attributes that business processes expect
                if 'watchlist_symbols' in self.process_args:
                    self.watchlist_symbols = self.process_args['watchlist_symbols']
                elif 'symbols' in self.process_args:
                    self.symbols = self.process_args['symbols']

                # Add process-specific attributes based on class name
                class_name = self.original_class.__name__

                # Common attributes
                self.last_cycle_time = None
                self.last_analysis_time = None
                self.last_check_time = None
                self.analysis_count = 0
                self.regime_checks_count = 0
                self.health_checks_count = 0
                self.signals_sent_count = 0
                self.alerts_sent_count = 0
                self.current_regime = 'UNKNOWN'
                self.regime_confidence = 0.0
                self.last_regime_update = None
                self.active_strategies = {}
                self.decision_queue = []
                self.analysis_interval = self.process_args.get('analysis_interval', 300)
                self.check_interval = self.process_args.get('check_interval', 60)
                
                # Shared state for pipeline communication
                self.shared_state = None
                
                # Strategy monitoring states for position health monitor
                self.strategy_monitoring_states = {}
                self.watchlist = []
                
                self._ensure_position_health_attributes()

                # Set all process_args as attributes for business class compatibility
                for key, value in self.process_args.items():
                    if not hasattr(self, key) and value is not None:
                        setattr(self, key, value)

                # Process-specific initialization
                if 'PositionHealthMonitor' in class_name:
                    self.check_interval = self.process_args.get('check_interval', 5)
                    self.active_strategies = {}

                elif 'EnhancedNewsPipelineProcess' in class_name:
                    self.watchlist_symbols = self.process_args.get('watchlist_symbols', [])
                    from collections import deque
                    self.processing_history = deque(maxlen=50)
                    self.error_history = deque(maxlen=20)
                    self.last_sentiment_results = []
                    self.last_recommendations = {}
                    self.symbol_tracking = {symbol: {
                        'last_update': None,
                        'recommendation_count': 0,
                        'avg_confidence': 0.0,
                        'last_recommendation': None
                    } for symbol in self.watchlist_symbols}

                elif 'DecisionEngine' in class_name:
                    self.decision_queue = []
                    self.daily_execution_count = 0
                    self.last_daily_reset = datetime.now().date()

                elif 'StrategyAnalyzer' in class_name:
                    self.analysis_interval = self.process_args.get('analysis_interval', 150)
                    self.confidence_threshold = self.process_args.get('confidence_threshold', 70.0)
                    self.last_analysis_time = None
                    self.analysis_count = 0
                    self.signals_sent_count = 0

                elif 'MarketRegimeDetector' in class_name:
                    self.check_interval = self.process_args.get('check_interval', 60)
                    self.last_check_time = None
                    self.regime_checks_count = 0

                # Initialize Redis manager reference
                self.redis_manager = get_trading_redis_manager()
                # Create alias for business instances that expect trading_redis_manager
                self.trading_redis_manager = self.redis_manager

                # Initialize config for business logic
                config_dict = {}
                
                if 'config' in self.process_args and isinstance(self.process_args['config'], dict):
                    config_dict.update(self.process_args['config'])
                    self.logger.info(f"Using provided config dict with {len(config_dict)} parameters")
                else:
                    # Build config from attributes
                    if hasattr(self, 'watchlist_symbols'):
                        config_dict['watchlist_symbols'] = self.watchlist_symbols
                    if hasattr(self, 'symbols'):
                        config_dict['symbols'] = self.symbols
                    if hasattr(self, 'analysis_interval'):
                        config_dict['analysis_interval'] = self.analysis_interval
                    if hasattr(self, 'check_interval'):
                        config_dict['check_interval'] = self.check_interval
                    
                    # Add additional config from process_args
                    for key, value in self.process_args.items():
                        if key != 'config' and key not in config_dict and value is not None:
                            config_dict[key] = value
                    
                    # Add common config parameters
                    if 'hours_back' not in config_dict:
                        config_dict['hours_back'] = self.process_args.get('hours_back', 24)
                
                # Create config object that supports both dict and attribute access
                self.config = type('Config', (), config_dict)()

                self.logger.info(f"Business attributes initialized for {class_name}")

            except Exception as e:
                self.logger.error(f"Error initializing business attributes: {e}")

        def _ensure_position_health_attributes(self):
            """Ensure all required attributes exist for position health monitoring"""
            try:
                required_attrs = {
                    'strategy_monitoring_states': {},
                    'watchlist': [],
                    'active_strategies': {},
                    'health_checks_count': 0,
                    'last_health_check': None
                }
                
                for attr, default_value in required_attrs.items():
                    if not hasattr(self, attr):
                        setattr(self, attr, default_value)
                        self.logger.info(f"Initialized missing attribute: {attr}")
                
                self.logger.debug("Position health attributes validation completed")
                
            except Exception as e:
                self.logger.error(f"Error ensuring position health attributes: {e}")

        def _synchronize_attributes_to_business_instance(self):
            """Synchronize wrapper attributes to business instance"""
            if not hasattr(self, '_business_instance') or self._business_instance is None:
                return
            
            # Define attributes that need to be synchronized
            critical_attributes = [
                'strategy_monitoring_states',
                'watchlist', 
                'active_strategies',
                'shared_state',
                'check_interval',
                'watchlist_symbols',
                'processing_history',
                'error_history',
                'last_sentiment_results',
                'last_recommendations',
                'symbol_tracking',
                'redis_manager',
                'trading_redis_manager'
            ]
            
            synchronized_count = 0
            for attr_name in critical_attributes:
                if hasattr(self, attr_name):
                    try:
                        attr_value = getattr(self, attr_name)
                        setattr(self._business_instance, attr_name, attr_value)
                        synchronized_count += 1
                        self.logger.debug(f"Synchronized attribute '{attr_name}' to business instance")
                    except Exception as e:
                        self.logger.warning(f"Could not synchronize attribute '{attr_name}': {e}")
            
            # Also synchronize process_args as attributes
            if hasattr(self, 'process_args') and self.process_args:
                for key, value in self.process_args.items():
                    if not hasattr(self._business_instance, key) and value is not None:
                        try:
                            setattr(self._business_instance, key, value)
                            synchronized_count += 1
                            self.logger.debug(f"Synchronized process_arg '{key}' to business instance")
                        except Exception as e:
                            self.logger.warning(f"Could not synchronize process_arg '{key}': {e}")
            
            self.logger.info(f"Attribute synchronization complete: {synchronized_count} attributes synchronized")

        def _initialize(self):
            """Initialize the wrapped process"""
            try:
                # Call the business class's initialize method if available
                if hasattr(self, '_business_initialize'):
                    self.logger.info(f"Calling business class initialization for {self.process_id}")
                    self._business_initialize()
                    self.logger.info(f"Business class initialization completed for {self.process_id}")
                
                self.logger.info(f"Process {self.process_id} initialized successfully")
            except Exception as e:
                self.logger.error(f"Error initializing process: {e}")
                raise

        def _execute(self):
            """Execute the main process logic"""
            try:
                # Call the business logic directly
                if hasattr(self, '_business_process_cycle'):
                    # Call the business process cycle method
                    self._business_process_cycle()
                    self.logger.critical(f"REAL BUSINESS LOGIC executed for {self.original_class.__name__}")

                    # Send colored output to main terminal
                    self._send_colored_output_to_main_terminal()

                    # Also log locally
                    try:
                        import os, sys
                        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
                        if project_root not in sys.path:
                            sys.path.append(project_root)
                        from colored_print import success
                        success(f"REAL BUSINESS: {self.process_id} executed successfully")
                    except:
                        print(f"[SUCCESS REAL BUSINESS] {self.process_id} executed successfully")
                else:
                    # No business method available
                    self.logger.critical(f"CRITICAL: Real business logic method not available for {self.process_id}")
                    self.logger.critical(f"Process {self.process_id} cannot execute real business functions")
                    self.logger.error(f"Business method binding failed - no _business_process_cycle method found")

                    # Just indicate the failure and pause
                    self.logger.critical(f"SKIPPING EXECUTION: No real business logic available")

                    # Brief pause to prevent spinning
                    time.sleep(2)

            except Exception as e:
                self.logger.error(f"Error in execute: {e}")
                self.error_count += 1

                # Brief pause on error to prevent spinning
                time.sleep(1)

        def _send_colored_output_to_main_terminal(self):
            """Send colored business execution output to main terminal via Redis"""
            try:
                # Don't interfere with OutputCoordinator processes
                outputcoordinator_processes = [
                    'enhanced_news_pipeline',
                    'decision_engine', 
                    'position_health_monitor',
                    'strategy_analyzer',
                    'finbert_sentiment',
                    'xgboost_engine'
                ]
                
                process_uses_outputcoordinator = any(proc in self.process_id.lower() for proc in outputcoordinator_processes)
                
                if process_uses_outputcoordinator:
                    # Let the business logic OutputCoordinator handle all outputs
                    self.logger.info(f"Process {self.process_id} uses OutputCoordinator - skipping wrapper output")
                    return
                
                # For legacy processes, send wrapper messages
                from Friren_V1.multiprocess_infrastructure.trading_redis_manager import create_process_message, MessagePriority
                
                # Get execution details from business instance
                message_text = "Business process executed successfully"
                color_type = "communication"
                
                # Extract real execution data from business instance for legacy processes
                if hasattr(self, '_business_instance') and self._business_instance:
                    try:
                        if 'legacy_news' in self.process_id.lower():
                            # Only for legacy news processes
                            if hasattr(self._business_instance, 'last_collected_articles') and hasattr(self._business_instance, 'watchlist_symbols'):
                                articles = getattr(self._business_instance, 'last_collected_articles', [])
                                symbols = getattr(self._business_instance, 'watchlist_symbols', [])
                                
                                if articles:
                                    article_info = []
                                    for art in articles[:2]:
                                        title = getattr(art, 'title', 'Unknown Article')[:40]
                                        symbol = getattr(art, 'symbol', 'MARKET')
                                        source = getattr(art, 'source', 'Unknown Source')
                                        article_info.append(f"'{title}' ({symbol} from {source})")
                                    
                                    if len(articles) == 1:
                                        message_text = f"News collected: {article_info[0]}"
                                    else:
                                        message_text = f"News collected: {article_info[0]}, +{len(articles)-1} more"
                                elif symbols:
                                    symbols_str = ', '.join(symbols[:3]) if len(symbols) <= 3 else f"{', '.join(symbols[:2])}, +{len(symbols)-2} more"
                                    message_text = f"News collection: Scanning {symbols_str} - no new articles found"
                                else:
                                    message_text = "News collection: No symbols configured for monitoring"
                            color_type = "news"
                        else:
                            # For other legacy processes
                            message_text = f"Legacy process {self.process_id} completed execution"
                            color_type = "communication"
                            
                    except Exception as attr_error:
                        self.logger.debug(f"Could not extract legacy execution details: {attr_error}")
                        message_text = f"Process {self.process_id} completed execution"
                        color_type = "communication"

                # Create message data for main terminal
                message_data = {
                    'process_id': self.process_id,
                    'output': message_text,
                    'color_type': color_type,
                    'timestamp': datetime.now().isoformat()
                }

                # Create Redis message for terminal output
                redis_message = create_process_message(
                    sender=self.process_id,
                    recipient='main_terminal',
                    message_type='COLORED_OUTPUT',
                    data=message_data,
                    priority=MessagePriority.HIGH
                )

                # Send via Redis to terminal_output queue
                if hasattr(self, 'redis_manager') and self.redis_manager:
                    self.redis_manager.send_message(redis_message, "terminal_output")
                    self.logger.info(f"Legacy wrapper output sent to main terminal: {message_text}")
                else:
                    self.logger.warning("Redis manager not available - cannot send colored output")

                # Also log locally
                self.logger.critical(f"REAL BUSINESS LOGIC executed for {self.original_class.__name__}")

            except Exception as e:
                # Fallback to logging only if Redis communication fails
                self.logger.error(f"Redis communication failed: {e}")
                self.logger.critical(f"FALLBACK: {self.process_id} business logic completed locally")
                
                # Emergency fallback - try to print directly
                try:
                    print(f"[{self.process_id}] Business logic executed successfully")
                except:
                    pass

        def _cleanup(self):
            """Cleanup the wrapped process"""
            try:
                # Cleanup business instance if created
                if hasattr(self, '_business_instance') and self._business_instance:
                    try:
                        if hasattr(self._business_instance, '_cleanup'):
                            self._business_instance._cleanup()
                        elif hasattr(self._business_instance, 'cleanup'):
                            self._business_instance.cleanup()
                        elif hasattr(self._business_instance, 'stop'):
                            self._business_instance.stop()
                        self.logger.info(f"Business instance cleaned up for {self.process_id}")
                    except Exception as e:
                        self.logger.warning(f"Could not cleanup business instance: {e}")
                    finally:
                        self._business_instance = None

                # Call parent cleanup
                super()._cleanup()
                self.logger.info(f"Process {self.process_id} cleanup completed")
            except Exception as e:
                self.logger.error(f"Error in cleanup: {e}")

    return RedisCompatibleProcess

def main():
    """Main entry point for Redis subprocess"""
    if len(sys.argv) != 2:
        print("ERROR: Usage: python redis_process_runner.py <config_file>", file=sys.stderr)
        sys.exit(1)

    config_file = sys.argv[1]

    try:
        # Load configuration
        with open(config_file, 'r') as f:
            config = json.load(f)

        process_id = config['process_id']
        module_path = config['module_path']
        class_name = config['class_name']
        process_args = config['process_args']
        heartbeat_interval = config.get('heartbeat_interval', 30)

        # Setup logging
        logger = setup_logging(process_id)
        logger.info(f"REDIS_SUBPROCESS: Starting process {process_id}")
        logger.info(f"REDIS_SUBPROCESS: Module: {module_path}, Class: {class_name}")
        logger.info(f"REDIS_SUBPROCESS: Args: {process_args}")

        # Load the original process class
        logger.info(f"REDIS_SUBPROCESS: Loading process class...")
        original_class = load_process_class(module_path, class_name)
        logger.info(f"REDIS_SUBPROCESS: Process class loaded successfully")

        # Create Redis-compatible wrapper
        logger.info(f"REDIS_SUBPROCESS: Creating Redis-compatible wrapper...")
        redis_process_class = create_redis_compatible_process(
            original_class,
            process_id,
            process_args,
            heartbeat_interval
        )

        # Create process instance
        logger.info(f"REDIS_SUBPROCESS: Creating process instance...")

        # Filter out process_id from args since we pass it separately
        filtered_args = {k: v for k, v in process_args.items() if k != 'process_id'}

        process_instance = redis_process_class(
            process_id=process_id,
            heartbeat_interval=heartbeat_interval,
            **filtered_args
        )

        logger.info(f"REDIS_SUBPROCESS: Process instance created")

        # Start the process
        logger.info(f"REDIS_SUBPROCESS: Starting process main loop...")
        process_instance.start()

        # Keep the process alive
        logger.info(f"REDIS_SUBPROCESS: Process {process_id} running, entering main loop...")

        try:
            # Wait for the process to complete
            if process_instance._main_thread:
                process_instance._main_thread.join()
        except KeyboardInterrupt:
            logger.info(f"REDIS_SUBPROCESS: Received interrupt signal")
        except Exception as e:
            logger.error(f"REDIS_SUBPROCESS: Error in main execution: {e}")

        # Stop the process
        logger.info(f"REDIS_SUBPROCESS: Stopping process {process_id}")
        try:
            process_instance.stop()
        except Exception as e:
            logger.error(f"REDIS_SUBPROCESS: Error stopping process: {e}")

        logger.info(f"REDIS_SUBPROCESS: Process {process_id} finished")

    except Exception as e:
        error_msg = f"REDIS_SUBPROCESS: Fatal error in process runner: {e}"
        print(error_msg, file=sys.stderr)
        if 'logger' in locals():
            logger.error(error_msg)
        sys.exit(1)

    finally:
        # Cleanup config file
        try:
            if os.path.exists(config_file):
                os.unlink(config_file)
        except Exception as e:
            print(f"REDIS_SUBPROCESS: Could not remove config file: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()