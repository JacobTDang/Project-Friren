"""
process_initializer.py

Process initialization and component setup
"""

import logging
from typing import Optional
from datetime import datetime

# Import infrastructure components (Redis only - production)
# FAIL FAST: No try...except - let ImportError crash the system immediately
from Friren_V1.trading_engine.portfolio_manager.tools.account_manager import AccountManager
from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
from Friren_V1.trading_engine.portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface

# PRODUCTION: Redis required
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager, TradingRedisManager
from Friren_V1.multiprocess_infrastructure.redis_process_manager import RedisProcessManager, ProcessConfig, RestartPolicy
REDIS_AVAILABLE = True
print("DEBUG: Redis infrastructure required and available")

INFRASTRUCTURE_AVAILABLE = True
print("DEBUG: Infrastructure imports successful")

# Import trading components
# FAIL FAST: No try...except - let ImportError crash the system immediately
from Friren_V1.trading_engine.portfolio_manager.decision_engine.decision_engine import EnhancedMarketDecisionEngineProcess as MarketDecisionEngine
from Friren_V1.trading_engine.portfolio_manager.decision_engine.execution_orchestrator import ExecutionOrchestrator
from Friren_V1.trading_engine.portfolio_manager.processes.position_health_monitor import PositionHealthMonitor
from Friren_V1.trading_engine.portfolio_manager.processes.strategy_analyzer_process import StrategyAnalyzerProcess
# REMOVED: FinBERTSentimentProcess (redundant - enhanced_news_pipeline handles FinBERT)
from Friren_V1.trading_engine.portfolio_manager.processes.enhanced_news_pipeline_process import EnhancedNewsPipelineProcess, get_default_pipeline_config
from Friren_V1.trading_engine.portfolio_manager.processes.market_regime_detector import MarketRegimeDetector
TRADING_COMPONENTS_AVAILABLE = True
print("DEBUG: Trading components imports successful")


class ProcessInitializer:
    """Handles all process initialization and component setup"""

    def __init__(self, config, status, logger):
        self.config = config
        self.status = status
        self.logger = logger

        # Component references
        self.process_manager: Optional[Any] = None
        self.account_manager: Optional[AccountManager] = None
        self.execution_orchestrator: Optional[ExecutionOrchestrator] = None
        self.redis_manager: Optional[Any] = None

    def initialize_all_components(self):
        """Initialize all system components (Database access only during startup)"""
        self.logger.info("Initializing all system components...")

        try:
            # Initialize core infrastructure
            self._initialize_shared_resources()
            self._initialize_account_manager()
            self._initialize_process_manager()
            self._initialize_execution_orchestrator()

            # PRODUCTION: Load initial account data from APIs during startup
            self.logger.info("Loading initial account data for production...")
            try:
                if self.account_manager:
                    # Force refresh during startup to get real account data
                    snapshot = self.account_manager.get_account_snapshot(force_refresh=True)
                    self.logger.info(f"Initial account snapshot: Portfolio=${snapshot.portfolio_value:,.2f}, Cash=${snapshot.cash:,.2f}")

                    # SYNC ALPACA POSITIONS TO DATABASE
                    self.logger.info("Syncing Alpaca positions to database...")
                    sync_success = self.account_manager.sync_alpaca_positions_to_database()
                    if sync_success:
                        self.logger.info("Successfully synced Alpaca positions to database")
                    else:
                        self.logger.warning("Failed to sync Alpaca positions to database")

            except Exception as e:
                self.logger.error(f"CRITICAL: Could not load initial account data: {e}")
                raise RuntimeError("Production system requires valid account data at startup")

            self.logger.info("All components initialized successfully")

        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise

    def get_components(self):
        """Return initialized components"""
        return {
            'process_manager': self.process_manager,
            'account_manager': self.account_manager,
            'execution_orchestrator': self.execution_orchestrator,
            'redis_manager': self.redis_manager
        }

    def _initialize_shared_resources(self):
        """Initialize Redis shared resources (production only)"""
        try:
            # PRODUCTION: Redis required
            self.redis_manager = get_trading_redis_manager()
            system_status = self.redis_manager.get_system_status()
            self.logger.info("Redis-based shared resources initialized successfully")
            self.logger.info(f"Redis manager: {type(self.redis_manager).__name__}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Redis shared resources: {e}")
            self.logger.error("PRODUCTION FAILURE: Redis is required for system operation")
            raise RuntimeError(f"Redis initialization failed - system cannot operate: {e}")

    def _initialize_account_manager(self):
        """Initialize account manager - PRODUCTION ONLY"""
        self.logger.info("Initializing production account manager...")

        try:
            from ..tools.account_manager import AccountManager
            from ..tools.db_manager import TradingDBManager
            from ..tools.alpaca_interface import SimpleAlpacaInterface

            # Initialize real components for production
            db_manager = TradingDBManager()
            alpaca_interface = SimpleAlpacaInterface()

            # Create real account manager
            self.account_manager = AccountManager(
                db_manager=db_manager,
                alpaca_interface=alpaca_interface,
                cache_duration=30  # 30-second cache for production
            )

            self.logger.info("Production account manager initialized successfully")

        except Exception as e:
            self.logger.error(f"CRITICAL: Could not initialize production account manager: {e}")
            raise RuntimeError(f"Production system requires real account manager: {e}")

    def _initialize_process_manager(self):
        """Initialize and register all processes"""
        self.logger.info("Starting process manager initialization")

        try:
            # PRODUCTION: Redis-based ProcessManager required
            self.logger.info("Creating Redis ProcessManager (production)")
            self.process_manager = RedisProcessManager(
                max_processes=5,  # ALLOW: 5 critical processes with queue rotation for memory management on t3.micro
                enable_queue_rotation=True,   # ENABLE: Queue rotation for 850MB memory target on t3.micro
                cycle_time_seconds=0.0   # Event-based rotation (0 = no time limits)
            )
            self.logger.info("Redis ProcessManager created successfully with CONTINUOUS execution enabled")

            # MEMORY OPTIMIZED: Limit to 5 critical processes to reduce RAM usage
            self.logger.info("MEMORY OPTIMIZED MODE: Limiting to 5 most critical processes")
            self.logger.info("REASON: Enhanced news pipeline critical for sentiment analysis")
            self.logger.info("SOLUTION: Queue-based process execution with health monitor first, then rotating queue")
            
            # TOP 5 CRITICAL PROCESSES - health monitor first, then add enhanced news pipeline
            processes = [
                ('position_health_monitor', PositionHealthMonitor),  # FIRST - provides system health data
                ('decision_engine', MarketDecisionEngine),
                ('enhanced_news_pipeline', EnhancedNewsPipelineProcess),  # CRITICAL - has integrated FinBERT analysis
                ('strategy_analyzer', StrategyAnalyzerProcess),
                ('market_regime_detector', MarketRegimeDetector),  # CRITICAL - provides market regime data for strategies
                # REMOVED: sentiment_analyzer (redundant - enhanced_news_pipeline handles FinBERT)
            ]

            for process_id, process_class in processes:
                if process_id == 'strategy_analyzer':
                    # Strategy analyzer with reduced intervals for more active testing
                    config = ProcessConfig(
                        process_class=process_class,
                        process_id=process_id,
                        restart_policy=RestartPolicy.ON_FAILURE,
                        max_restarts=3,  # Allow some restarts for Redis processes
                        restart_delay_seconds=5,
                        health_check_interval=self.config.health_check_interval,
                        process_args={
                            'analysis_interval': 30,  # 30 seconds instead of 150
                            'confidence_threshold': 50.0,  # 50% instead of 70%
                            'symbols': self.config.symbols
                        }
                    )
                elif process_id == 'position_health_monitor':
                    # Position health monitor with reduced intervals
                    config = ProcessConfig(
                        process_class=process_class,
                        process_id=process_id,
                        restart_policy=RestartPolicy.ON_FAILURE,
                        max_restarts=3,  # Allow some restarts for Redis processes
                        restart_delay_seconds=5,
                        health_check_interval=self.config.health_check_interval,
                        process_args={
                            'check_interval': 60,  # Production interval - 60 seconds
                            'symbols': self.config.symbols
                        }
                    )
                elif process_id == 'enhanced_news_pipeline':
                    # Enhanced news pipeline for news collection and sentiment analysis
                    config = ProcessConfig(
                        process_class=process_class,
                        process_id=process_id,
                        restart_policy=RestartPolicy.ON_FAILURE,
                        max_restarts=3,
                        restart_delay_seconds=5,
                        health_check_interval=self.config.health_check_interval,
                        process_args={
                            'watchlist_symbols': self.config.symbols,
                            'memory_limit_mb': 1200,  # CRITICAL FIX: Set 1200MB for FinBERT + XGBoost + News Collection
                            'config': {  # CRITICAL FIX: Add missing config dict with all required parameters
                                'cycle_interval_minutes': 1,  # Run every 1 minute for testing
                                'batch_size': 4,
                                'max_memory_mb': 1200,  # Match memory_limit_mb
                                'max_articles_per_symbol': 12,
                                'hours_back': 6,
                                'quality_threshold': 0.7,
                                'finbert_batch_size': 4,
                                'min_confidence_threshold': 0.6,
                                'enable_xgboost': True,
                                'recommendation_threshold': 0.65,
                                'enable_caching': True,
                                'cache_ttl_minutes': 30
                            }
                        }
                    )
                # REMOVED: sentiment_analyzer configuration (redundant process)
                elif process_id == 'decision_engine':
                    config = ProcessConfig(
                        process_class=process_class,
                        process_id=process_id,
                        restart_policy=RestartPolicy.ON_FAILURE,
                        max_restarts=3,
                        restart_delay_seconds=5,
                        health_check_interval=self.config.health_check_interval,
                        startup_timeout=45,  # Reduced timeout - 45 seconds
                        process_args={}
                    )
                elif process_id == 'market_regime_detector':
                    # Market regime detector for strategy context
                    config = ProcessConfig(
                        process_class=process_class,
                        process_id=process_id,
                        restart_policy=RestartPolicy.ON_FAILURE,
                        max_restarts=3,
                        restart_delay_seconds=5,
                        health_check_interval=self.config.health_check_interval,
                        process_args={
                            'check_interval': 300,  # 5 minutes - regime detection doesn't need high frequency
                            'symbols': ['SPY', 'QQQ', 'IWM', '^VIX']  # Major market indices
                        }
                    )

                self.process_manager.register_process(config)
                self.logger.info(f"Registered process: {process_id}")

        except Exception as e:
            self.logger.error(f"FAILED to create ProcessManager: {e}")
            self.logger.error("NO FALLBACK - System must use real components")
            raise RuntimeError(f"ProcessManager initialization failed: {e}")

    # REMOVED: _create_fallback_process_manager - NO FALLBACKS ALLOWED

    def _initialize_execution_orchestrator(self):
        """Initialize the execution orchestrator"""
        try:
            self.logger.info("Initializing real execution orchestrator...")
            self.execution_orchestrator = ExecutionOrchestrator()
            self.logger.info("Real execution orchestrator initialized successfully")
            self.logger.info("Execution orchestrator initialized")
        except Exception as e:
            self.logger.error(f"CRITICAL ERROR initializing execution orchestrator: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            self.logger.error("NO FALLBACK - System must use real execution orchestrator")
            raise RuntimeError(f"Execution orchestrator initialization failed: {e}")

    def _validate_system_readiness(self):
        """Validate that all required components are ready"""
        issues = []

        if not self.execution_orchestrator:
            issues.append("Execution orchestrator not initialized")

        if not self.config.symbols:
            issues.append("No symbols configured for trading")

        if issues:
            raise RuntimeError(f"System readiness validation failed: {issues}")

    def start_processes(self):
        """Start all registered processes (limited to 5 for memory optimization)"""
        if self.process_manager:
            # Define dependency order - decision engine should start first
            # INCLUDE NEWS PIPELINE - Critical for business logic execution
            dependency_order = [
                'decision_engine',
                'position_health_monitor',
                'market_regime_detector',  # CRITICAL: Must start before strategy analyzer
                'enhanced_news_pipeline',  # CRITICAL: Has built-in FinBERT analysis
                'strategy_analyzer'
            ]

            self.process_manager.start_all_processes(dependency_order)
            self.logger.info("All 5 critical processes started with market regime detection and integrated news pipeline")
        else:
            self.logger.warning("Process manager not available")

    def sync_positions_to_symbol_coordinator(self, symbol_coordinator):
        """Sync positions from database to symbol coordinator after it's initialized"""
        try:
            if not self.account_manager or not symbol_coordinator:
                self.logger.warning("Cannot sync positions: account manager or symbol coordinator not available")
                return False

            self.logger.info("=== POSITION SYNC DEBUG START ===")

            # Check symbol coordinator status before sync
            initial_status = symbol_coordinator.get_coordination_status()
            self.logger.info(f"Symbol coordinator status before sync: {initial_status}")

            # Debug active symbols before sync
            if 'symbols' in initial_status:
                active_symbols_before = [symbol for symbol, data in initial_status['symbols'].items() 
                                       if data.get('position', 0) > 0]
                self.logger.info(f"Active positions before sync: {active_symbols_before}")
            else:
                self.logger.info("No symbols found in symbol coordinator before sync")

            # Get database holdings
            holdings = self.account_manager.db_manager.get_holdings(active_only=True)
            self.logger.info(f"Database holdings: {holdings}")

            # Sync positions
            self.logger.info("Syncing positions to symbol coordinator...")
            symbol_coordinator.sync_positions_from_database(self.account_manager.db_manager)

            # Check symbol coordinator status after sync
            after_sync_status = symbol_coordinator.get_coordination_status()
            self.logger.info(f"Symbol coordinator status after sync: {after_sync_status}")

            # Analyze symbol coordinator status after sync
            if 'symbols' in after_sync_status:
                active_symbols = []
                pending_decisions = 0

                for symbol, symbol_data in after_sync_status['symbols'].items():
                    position = symbol_data.get('position', 0.0)
                    if position > 0:
                        active_symbols.append(symbol)

                    intensity = symbol_data.get('intensity', 'passive')
                    if intensity == 'intensive':
                        pending_decisions += 1

                self.logger.info(f"DEBUG MONITORING OUTPUT: SYMBOL COORDINATOR: Active={len(active_symbols)} | Pending={pending_decisions}")
                self.logger.info(f"Active symbols after sync: {active_symbols}")
            else:
                self.logger.info("No symbols found in symbol coordinator after sync")

            self.logger.info("=== POSITION SYNC DEBUG END ===")
            self.logger.info("Successfully synced positions to symbol coordinator")
            return True

        except Exception as e:
            self.logger.error(f"Failed to sync positions to symbol coordinator: {e}")
            import traceback
            traceback.print_exc()
            return False
