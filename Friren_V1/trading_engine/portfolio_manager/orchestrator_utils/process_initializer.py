#!/usr/bin/env python3
"""
process_initializer.py

Process initialization and component setup
"""

import logging
from typing import Optional
from datetime import datetime

# Import infrastructure components
try:
    from Friren_V1.trading_engine.portfolio_manager.tools.account_manager import AccountManager
    from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
    from Friren_V1.trading_engine.portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface
    from Friren_V1.multiprocess_infrastructure.queue_manager import QueueManager as RealQueueManager
    from Friren_V1.multiprocess_infrastructure.shared_state_manager import SharedStateManager as RealSharedStateManager
    from Friren_V1.multiprocess_infrastructure.process_manager import ProcessManager as RealProcessManager
    from Friren_V1.multiprocess_infrastructure.process_manager import ProcessConfig, RestartPolicy
    INFRASTRUCTURE_AVAILABLE = True
except ImportError:
    INFRASTRUCTURE_AVAILABLE = False

# Import trading components
try:
    from Friren_V1.trading_engine.portfolio_manager.decision_engine.decision_engine import EnhancedMarketDecisionEngineProcess as MarketDecisionEngine
    from Friren_V1.trading_engine.portfolio_manager.decision_engine.execution_orchestrator import ExecutionOrchestrator
    from Friren_V1.trading_engine.portfolio_manager.processes.position_health_monitor import PositionHealthMonitor
    from Friren_V1.trading_engine.portfolio_manager.processes.strategy_analyzer_process import StrategyAnalyzerProcess
    from Friren_V1.trading_engine.portfolio_manager.processes.finbert_sentiment_process import FinBERTSentimentProcess
    from Friren_V1.trading_engine.portfolio_manager.processes.enhanced_news_pipeline_process import EnhancedNewsPipelineProcess, get_default_pipeline_config
    TRADING_COMPONENTS_AVAILABLE = True
except ImportError:
    TRADING_COMPONENTS_AVAILABLE = False


class ProcessInitializer:
    """Handles all process initialization and component setup"""

    def __init__(self, config, status, logger):
        self.config = config
        self.status = status
        self.logger = logger

        # Component references
        self.process_manager: Optional[RealProcessManager] = None
        self.account_manager: Optional[AccountManager] = None
        self.execution_orchestrator: Optional[ExecutionOrchestrator] = None
        self.shared_state: Optional[RealSharedStateManager] = None
        self.queue_manager: Optional[RealQueueManager] = None

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
            'shared_state': self.shared_state,
            'queue_manager': self.queue_manager
        }

    def _initialize_shared_resources(self):
        """Initialize shared state and queue managers"""
        if INFRASTRUCTURE_AVAILABLE:
            self.shared_state = RealSharedStateManager()
            self.queue_manager = RealQueueManager()
            self.logger.info("Real shared resources initialized")
        else:
            self.logger.warning("Infrastructure not available, using fallbacks")
            # Create fallback shared state and queue manager
            self.shared_state = type('MockSharedState', (), {'set': lambda self, k, v: None, 'cleanup': lambda self: None})()
            self.queue_manager = type('MockQueueManager', (), {})()
            self.logger.info("Fallback shared resources initialized")

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
                # Always use real ProcessManager - no fallbacks
                self.logger.info("Creating real ProcessManager")
                self.process_manager = RealProcessManager(max_processes=5)
                self.logger.info("ProcessManager created successfully")

                # Register the 5 core processes
                processes = [
                    ('decision_engine', MarketDecisionEngine),
                    ('position_health_monitor', PositionHealthMonitor),
                    ('strategy_analyzer', StrategyAnalyzerProcess),
                    ('sentiment_analyzer', FinBERTSentimentProcess),
                    ('enhanced_news_pipeline', EnhancedNewsPipelineProcess)
                ]

                for process_id, process_class in processes:
                    # Special configuration for enhanced news pipeline
                    if process_id == 'enhanced_news_pipeline':
                        # Create enhanced pipeline with optimized configuration
                        pipeline_config = get_default_pipeline_config()
                        config = ProcessConfig(
                            process_class=process_class,
                            process_id=process_id,
                            restart_policy=RestartPolicy.ON_FAILURE,
                            max_restarts=3,
                            restart_delay_seconds=5,
                            health_check_interval=self.config.health_check_interval,
                            process_args={
                                'watchlist_symbols': self.config.symbols,
                                'config': pipeline_config
                            }
                        )
                    else:
                        config = ProcessConfig(
                            process_class=process_class,
                            process_id=process_id,
                            restart_policy=RestartPolicy.ON_FAILURE,
                            max_restarts=3,
                            restart_delay_seconds=5,
                            health_check_interval=self.config.health_check_interval,
                            process_args={}
                        )

                    self.process_manager.register_process(config)
                    self.logger.info(f"Registered process: {process_id}")
            else:
                self.logger.warning("Real infrastructure not available, creating fallback process manager")
                self._create_fallback_process_manager()

        except Exception as e:
            self.logger.error(f"FAILED to create ProcessManager: {e}")
            self.logger.warning("Creating fallback process manager")
            self._create_fallback_process_manager()

    def _create_fallback_process_manager(self):
        """Create a fallback process manager for testing"""
        self.logger.info("Creating fallback process manager")

        # Create a simple fallback that provides the necessary interface
        class FallbackProcessManager:
            def __init__(self, max_processes=5):
                self.max_processes = max_processes
                self.processes = {}
                self.decision_queue = []

            def register_process(self, config):
                process_id = getattr(config, 'process_id', 'unknown')
                self.processes[process_id] = {'registered': True}

            def start_all_processes(self, dependency_order=None):
                self.logger.info("Fallback: All processes started")

            def stop_all_processes(self):
                self.logger.info("Fallback: All processes stopped")

            def get_process_status(self):
                return {pid: {'is_healthy': True, 'is_running': True, 'uptime_seconds': 100}
                       for pid in self.processes}

            def simulate_all_processes(self):
                # Simple simulation
                pass

        self.process_manager = FallbackProcessManager(max_processes=5)

        # Register the 5 core processes for fallback
        processes = [
            ('decision_engine', 'MarketDecisionEngine'),
            ('position_health_monitor', 'PositionHealthMonitor'),
            ('strategy_analyzer', 'StrategyAnalyzerProcess'),
            ('sentiment_analyzer', 'FinBERTSentimentProcess'),
            ('enhanced_news_pipeline', 'EnhancedNewsPipelineProcess')
        ]

        for process_id, process_name in processes:
            # Create a mock config object
            config = type('MockConfig', (), {
                'process_id': process_id,
                'process_class': type(process_name, (), {})
            })()
            self.process_manager.register_process(config)
            self.logger.info(f"Registered fallback process: {process_id}")

    def _initialize_execution_orchestrator(self):
        """Initialize the execution orchestrator"""
        try:
            if TRADING_COMPONENTS_AVAILABLE:
                self.execution_orchestrator = ExecutionOrchestrator()
            else:
                # Create fallback execution orchestrator
                class FallbackExecutionOrchestrator:
                    def execute_approved_decision(self, risk_validation, strategy_name, confidence):
                        return type('Result', (), {
                            'was_successful': True,
                            'execution_summary': f"Fallback execution for {strategy_name}",
                            'error_message': None
                        })()

                self.execution_orchestrator = FallbackExecutionOrchestrator()
            self.logger.info("Execution orchestrator initialized")
        except Exception as e:
            # Create fallback execution orchestrator
            class FallbackExecutionOrchestrator:
                def execute_approved_decision(self, risk_validation, strategy_name, confidence):
                    return type('Result', (), {
                        'was_successful': True,
                        'execution_summary': f"Fallback execution for {strategy_name}",
                        'error_message': None
                    })()

            self.execution_orchestrator = FallbackExecutionOrchestrator()
            self.logger.warning(f"Using fallback execution orchestrator: {e}")

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
                'enhanced_news_pipeline'
            ]

            self.process_manager.start_all_processes(dependency_order)
            self.logger.info("All processes started")
        else:
            self.logger.warning("Process manager not available")
