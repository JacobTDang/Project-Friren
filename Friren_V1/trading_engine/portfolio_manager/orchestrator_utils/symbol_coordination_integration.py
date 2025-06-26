#!/usr/bin/env python3
"""
symbol_coordination_integration.py

Symbol coordination system integration
"""

import logging
from datetime import datetime
from typing import Optional

try:
    from Friren_V1.trading_engine.portfolio_manager.symbol_coordination.symbol_config import SymbolMonitoringConfig, MonitoringIntensity
    from Friren_V1.trading_engine.portfolio_manager.symbol_coordination.symbol_coordinator import SymbolCoordinator
    from Friren_V1.trading_engine.portfolio_manager.symbol_coordination.resource_manager import ResourceManager, ResourceQuota
    from Friren_V1.trading_engine.portfolio_manager.symbol_coordination.message_router import MessageRouter
    SYMBOL_COORDINATION_AVAILABLE = True
except ImportError:
    SYMBOL_COORDINATION_AVAILABLE = False


class SymbolCoordinationIntegration:
    """Handles symbol coordination system integration"""

    def __init__(self, config, status, logger):
        self.config = config
        self.status = status
        self.logger = logger

        # Symbol coordination components
        self.symbol_coordinator: Optional[SymbolCoordinator] = None
        self.resource_manager: Optional[ResourceManager] = None
        self.message_router: Optional[MessageRouter] = None

    def initialize_symbol_coordination(self):
        """Initialize the symbol coordination system"""
        try:
            self.logger.info(f"=== SYMBOL COORDINATION INIT DEBUG ===")
            self.logger.info(f"symbol_coordination_enabled: {self.config.symbol_coordination_enabled}")
            self.logger.info(f"SYMBOL_COORDINATION_AVAILABLE: {SYMBOL_COORDINATION_AVAILABLE}")
            self.logger.info(f"config.symbols: {self.config.symbols}")
            
            if self.config.symbol_coordination_enabled and SYMBOL_COORDINATION_AVAILABLE:
                # Initialize resource manager first
                self._initialize_resource_manager()

                # Initialize message router
                self._initialize_message_router()

                # Create symbol coordinator with configuration
                self.symbol_coordinator = SymbolCoordinator(
                    total_api_budget=self.config.total_api_budget,
                    max_intensive_symbols=self.config.max_intensive_symbols
                )

                # Add configured symbols to coordinator
                self.logger.info(f"Adding {len(self.config.symbols)} symbols to coordinator...")
                for symbol in self.config.symbols:
                    self.logger.info(f"Processing symbol: {symbol}")
                    intensity = getattr(MonitoringIntensity, self.config.default_symbol_intensity.upper(), MonitoringIntensity.ACTIVE)
                    config = SymbolMonitoringConfig(
                        symbol=symbol,
                        monitoring_intensity=intensity,
                        api_call_budget=self.config.api_budget_per_symbol
                    )
                    success = self.symbol_coordinator.add_symbol(symbol, config)
                    if success:
                        self.logger.info(f"[SUCCESS] Successfully added symbol {symbol} to coordination with intensity {intensity}")

                        # Register symbol with resource manager and message router
                        if self.resource_manager:
                            self.resource_manager.allocate_resources(symbol, config)
                        if self.message_router:
                            self.message_router.register_symbol(symbol, config)
                    else:
                        self.logger.warning(f"[FAILED] Failed to add symbol {symbol} to coordination")
                
                # Check final status
                status = self.symbol_coordinator.get_coordination_status()
                self.logger.info(f"Final symbol coordinator status: {len(status['symbols'])} symbols added")
                for sym, data in status['symbols'].items():
                    self.logger.info(f"  {sym}: position={data.get('position', 0)}, intensity={data.get('intensity', 'unknown')}")

                # Start resource monitoring and message routing
                if self.resource_manager:
                    self.resource_manager.start_monitoring()
                if self.message_router:
                    self.message_router.start()

                # Update status
                self.status.symbol_coordination_enabled = True
                self.status.last_symbol_coordination_update = datetime.now()

                self.logger.info(f"Symbol coordination initialized with {len(self.config.symbols)} symbols")
            else:
                # Use fallback coordinator
                self._create_fallback_coordinators()
                self.status.symbol_coordination_enabled = False
                if not self.config.symbol_coordination_enabled:
                    self.logger.info("Symbol coordination disabled by configuration")
                else:
                    self.logger.warning("Symbol coordination not available, using fallback")

        except Exception as e:
            # Fallback to stub coordinator
            self._create_fallback_coordinators()
            self.status.symbol_coordination_enabled = False
            self.logger.error(f"Failed to initialize symbol coordination: {e}")
            self.logger.info("Using fallback symbol coordinator")

    def _create_fallback_coordinators(self):
        """Create fallback coordinators"""
        class FallbackSymbolCoordinator:
            def add_symbol(self, symbol, config):
                return True
            def get_next_symbol_to_process(self):
                return None
            def get_coordination_status(self):
                return {
                    'symbols': {},  # Fixed: was 'symbol_states', should be 'symbols'
                    'total_symbols': 0,
                    'intensive_symbols': 0,
                    'metrics': {}
                }

        class FallbackResourceManager:
            def allocate_resources(self, symbol, config):
                pass
            def start_monitoring(self):
                pass
            def stop_monitoring(self):
                pass

        class FallbackMessageRouter:
            def register_symbol(self, symbol, config):
                pass
            def route_message(self, symbol, message):
                return True
            def start(self):
                pass
            def stop(self):
                pass

        self.symbol_coordinator = FallbackSymbolCoordinator()
        self.resource_manager = FallbackResourceManager()
        self.message_router = FallbackMessageRouter()

    def _initialize_resource_manager(self):
        """Initialize the resource manager"""
        try:
            # Create resource quota based on system configuration
            quota = ResourceQuota(
                api_calls_per_hour=self.config.total_api_budget,
                memory_limit_mb=self.config.max_memory_mb,
                cpu_limit_percent=self.config.max_cpu_percent,
                api_buffer=self.config.api_rate_limit_buffer
            )

            self.resource_manager = ResourceManager(quota)
            self.logger.info("Resource manager initialized")

        except Exception as e:
            self.resource_manager = None
            self.logger.error(f"Failed to initialize resource manager: {e}")

    def _initialize_message_router(self):
        """Initialize the message router"""
        try:
            self.message_router = MessageRouter(self.resource_manager)
            self.logger.info("Message router initialized")

        except Exception as e:
            self.message_router = None
            self.logger.error(f"Failed to initialize message router: {e}")

    def update_symbol_coordination_status(self):
        """Update symbol coordination status metrics"""
        try:
            if not self.symbol_coordinator:
                return

            # Get coordination status
            coordination_status = self.symbol_coordinator.get_coordination_status()

            # Update system status
            self.status.symbol_coordination_enabled = True
            self.status.total_symbols_managed = len(self.config.symbols)
            self.status.last_symbol_coordination_update = datetime.now()

            # Update intensity counts from coordination status
            if 'symbol_states' in coordination_status:
                intensity_counts = {'intensive': 0, 'active': 0, 'passive': 0}
                for symbol_state in coordination_status['symbol_states'].values():
                    intensity = symbol_state.get('monitoring_intensity', 'active')
                    if intensity in intensity_counts:
                        intensity_counts[intensity] += 1

                self.status.intensive_symbols_count = intensity_counts['intensive']
                self.status.active_symbols_count = intensity_counts['active']
                self.status.passive_symbols_count = intensity_counts['passive']

        except Exception as e:
            self.logger.error(f"Error updating symbol coordination status: {e}")

    def get_components(self):
        """Return symbol coordination components"""
        return {
            'symbol_coordinator': self.symbol_coordinator,
            'resource_manager': self.resource_manager,
            'message_router': self.message_router
        }

    def stop_symbol_coordination(self):
        """Stop symbol coordination components"""
        try:
            if self.message_router:
                self.message_router.stop()
            if self.resource_manager:
                self.resource_manager.stop_monitoring()
        except Exception as e:
            self.logger.error(f"Error stopping symbol coordination: {e}")
