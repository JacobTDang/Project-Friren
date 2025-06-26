#!/usr/bin/env python3
"""
process_initializer.py

Process initialization and component setup
"""

import logging
from typing import Optional
from datetime import datetime

# Import infrastructure components (Redis or fallback)
# Always import fallback first
from Friren_V1.multiprocess_infrastructure.fallback_manager import get_fallback_manager

try:
    from Friren_V1.trading_engine.portfolio_manager.tools.account_manager import AccountManager
    from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
    from Friren_V1.trading_engine.portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface
    
    # Try Redis 
    try:
        from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager, TradingRedisManager
        from Friren_V1.multiprocess_infrastructure.redis_process_manager import RedisProcessManager, ProcessConfig, RestartPolicy
        REDIS_AVAILABLE = True
        print("DEBUG: Redis infrastructure available")
    except ImportError as redis_err:
        print(f"DEBUG: Redis not available ({redis_err}), using fallback")
        REDIS_AVAILABLE = False
        # Create fallback stubs when Redis not available
        class ProcessConfig:
            pass
        class RestartPolicy:
            pass
    
    INFRASTRUCTURE_AVAILABLE = True
    print("DEBUG: Infrastructure imports successful")
except ImportError as e:
    print(f"DEBUG: Infrastructure import failed: {e}")
    INFRASTRUCTURE_AVAILABLE = False
    REDIS_AVAILABLE = False

# Import trading components
try:
    from Friren_V1.trading_engine.portfolio_manager.decision_engine.decision_engine import EnhancedMarketDecisionEngineProcess as MarketDecisionEngine
    from Friren_V1.trading_engine.portfolio_manager.decision_engine.execution_orchestrator import ExecutionOrchestrator
    from Friren_V1.trading_engine.portfolio_manager.processes.position_health_monitor import PositionHealthMonitor
    from Friren_V1.trading_engine.portfolio_manager.processes.strategy_analyzer_process import StrategyAnalyzerProcess
    from Friren_V1.trading_engine.portfolio_manager.processes.finbert_sentiment_process import FinBERTSentimentProcess
    from Friren_V1.trading_engine.portfolio_manager.processes.enhanced_news_pipeline_process import EnhancedNewsPipelineProcess, get_default_pipeline_config
    from Friren_V1.trading_engine.portfolio_manager.processes.market_regime_detector import MarketRegimeDetector
    TRADING_COMPONENTS_AVAILABLE = True
    print("DEBUG: Trading components imports successful")
except ImportError as e:
    print(f"DEBUG: Trading components import failed: {e}")
    TRADING_COMPONENTS_AVAILABLE = False


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
        """Initialize shared resources (Redis or fallback)"""
        try:
            if REDIS_AVAILABLE:
                # Use Redis
                self.redis_manager = get_trading_redis_manager()
                system_status = self.redis_manager.get_system_status()
                self.logger.info("Redis-based shared resources initialized successfully")
                self.logger.info(f"Redis manager: {type(self.redis_manager).__name__}")
            else:
                # Use fallback
                self.redis_manager = get_fallback_manager()
                system_status = self.redis_manager.get_system_status()
                self.logger.info("Fallback shared resources initialized (Redis not available)")
                self.logger.info(f"Fallback manager: {type(self.redis_manager).__name__}")

        except Exception as e:
            self.logger.error(f"Failed to initialize shared resources: {e}")
            raise RuntimeError(f"Shared resource initialization failed: {e}")

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
            if INFRASTRUCTURE_AVAILABLE and TRADING_COMPONENTS_AVAILABLE:
                if REDIS_AVAILABLE:
                    # Create Redis-based ProcessManager
                    self.logger.info("Creating Redis ProcessManager")
                    self.process_manager = RedisProcessManager(
                        max_processes=6  # ULTRA PARALLEL: All 6 processes enabled
                    )
                    self.logger.info("Redis ProcessManager created successfully")
                else:
                    # Fallback: Skip process manager for now
                    self.logger.warning("Redis not available - skipping process manager (processes won't start)")
                    self.process_manager = None
                    return

                # ULTRA FIX: ENABLE ALL PROCESSES with Windows deadlock fixes
                self.logger.info("ULTRA PARALLEL MODE: Enabling ALL 6 processes with deadlock fixes")
                self.logger.info("REASON: User wants true parallelism - applying Windows-specific fixes")
                self.logger.info("SOLUTION: All processes enabled with ultra-minimal initialization")
                
                # ALL PROCESSES ENABLED with Windows fixes
                processes = [
                    ('decision_engine', MarketDecisionEngine),
                    ('position_health_monitor', PositionHealthMonitor),  
                    ('strategy_analyzer', StrategyAnalyzerProcess),
                    ('sentiment_analyzer', FinBERTSentimentProcess),
                    ('enhanced_news_pipeline', EnhancedNewsPipelineProcess),
                    ('market_regime_detector', MarketRegimeDetector)
                ]

                for process_id, process_class in processes:
                    # Special configuration for enhanced news pipeline
                    if process_id == 'enhanced_news_pipeline':
                        # Create enhanced pipeline with optimized configuration
                        pipeline_config = get_default_pipeline_config()
                        if not REDIS_AVAILABLE:
                            # Skip process registration when Redis not available
                            self.logger.warning("Skipping process registration - Redis not available")
                            return
                        config = ProcessConfig(
                            process_class=process_class,
                            process_id=process_id,
                            restart_policy=RestartPolicy.ON_FAILURE,
                            max_restarts=3,  # Allow some restarts for Redis processes
                            restart_delay_seconds=5,
                            health_check_interval=self.config.health_check_interval,
                            process_args={
                                'watchlist_symbols': self.config.symbols,
                                'config': pipeline_config
                            }
                        )
                    elif process_id == 'strategy_analyzer':
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
                                'check_interval': 5,  # 5 seconds instead of 10
                                'symbols': self.config.symbols
                            }
                        )
                    elif process_id == 'market_regime_detector':
                        # Market regime detector with reduced intervals
                        config = ProcessConfig(
                            process_class=process_class,
                            process_id=process_id,
                            restart_policy=RestartPolicy.ON_FAILURE,
                            max_restarts=3,  # Allow some restarts for Redis processes
                            restart_delay_seconds=5,
                            health_check_interval=self.config.health_check_interval,
                            process_args={
                                'check_interval': 60,  # 60 seconds instead of 300
                                'symbols': self.config.symbols
                            }
                        )
                    elif process_id == 'sentiment_analyzer':
                        # PRODUCTION: FinBERT sentiment analyzer needs longer startup for model loading
                        config = ProcessConfig(
                            process_class=process_class,
                            process_id=process_id,
                            restart_policy=RestartPolicy.ON_FAILURE,
                            max_restarts=3,
                            restart_delay_seconds=5,
                            health_check_interval=self.config.health_check_interval,
                            startup_timeout=300,  # 5 minutes for FinBERT model loading
                            process_args={
                                'analysis_interval': 30,
                                'confidence_threshold': 50.0,
                                'symbols': self.config.symbols
                            }
                        )
                    else:
                        config = ProcessConfig(
                            process_class=process_class,
                            process_id=process_id,
                            restart_policy=RestartPolicy.ON_FAILURE,
                            max_restarts=3,  # Allow some restarts for Redis processes
                            restart_delay_seconds=5,
                            health_check_interval=self.config.health_check_interval,
                            process_args={}
                        )

                    self.process_manager.register_process(config)
                    self.logger.info(f"Registered process: {process_id}")
            else:
                self.logger.error("Real infrastructure not available - SYSTEM MUST FAIL")
                raise RuntimeError("Real infrastructure components required - no fallback allowed")

        except Exception as e:
            self.logger.error(f"FAILED to create ProcessManager: {e}")
            self.logger.error("NO FALLBACK - System must use real components")
            raise RuntimeError(f"ProcessManager initialization failed: {e}")

    # REMOVED: _create_fallback_process_manager - NO FALLBACKS ALLOWED

    def _initialize_execution_orchestrator(self):
        """Initialize the execution orchestrator"""
        try:
            if TRADING_COMPONENTS_AVAILABLE:
                self.logger.info("Initializing real execution orchestrator...")
                self.execution_orchestrator = ExecutionOrchestrator()
                self.logger.info("Real execution orchestrator initialized successfully")
            else:
                self.logger.error("Trading components not available - SYSTEM MUST FAIL")
                raise RuntimeError("Real trading components required - no fallback allowed")
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
        """Start all registered processes"""
        if self.process_manager:
            # Define dependency order - decision engine should start first
            dependency_order = [
                'decision_engine',
                'position_health_monitor',
                'strategy_analyzer',
                'sentiment_analyzer',
                'enhanced_news_pipeline',
                'market_regime_detector'
            ]

            self.process_manager.start_all_processes(dependency_order)
            self.logger.info("All processes started")
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

            if 'symbols' in initial_status and 'AAPL' in initial_status['symbols']:
                aapl_before = initial_status['symbols']['AAPL']
                self.logger.info(f"AAPL position before sync: {aapl_before.get('position', 'NOT FOUND')}")
            else:
                self.logger.info("AAPL not found in symbol coordinator before sync")

            # Get database holdings
            holdings = self.account_manager.db_manager.get_holdings(active_only=True)
            self.logger.info(f"Database holdings: {holdings}")

            # Sync positions
            self.logger.info("Syncing positions to symbol coordinator...")
            symbol_coordinator.sync_positions_from_database(self.account_manager.db_manager)

            # Check symbol coordinator status after sync
            after_sync_status = symbol_coordinator.get_coordination_status()
            self.logger.info(f"Symbol coordinator status after sync: {after_sync_status}")

            if 'symbols' in after_sync_status and 'AAPL' in after_sync_status['symbols']:
                aapl_after = after_sync_status['symbols']['AAPL']
                self.logger.info(f"AAPL position after sync: {aapl_after.get('position', 'NOT FOUND')}")

                # Test the monitoring logic
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
                self.logger.info(f"Active symbols: {active_symbols}")
            else:
                self.logger.info("AAPL not found in symbol coordinator after sync")

            self.logger.info("=== POSITION SYNC DEBUG END ===")
            self.logger.info("Successfully synced positions to symbol coordinator")
            return True

        except Exception as e:
            self.logger.error(f"Failed to sync positions to symbol coordinator: {e}")
            import traceback
            traceback.print_exc()
            return False
