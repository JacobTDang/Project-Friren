"""
Symbol Coordinator

Main coordination class for managing per-symbol monitoring, resource allocation,
and decision routing in the enhanced multi-symbol trading system.
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict

from .symbol_config import (
    SymbolMonitoringConfig,
    SymbolState,
    SymbolResourceBudget,
    MonitoringIntensity,
    SymbolHealth
)


class SymbolCoordinator:
    """
    Central coordinator for symbol-specific monitoring and resource allocation.

    Manages per-symbol states, coordinates resource allocation, and handles
    dynamic monitoring intensity adjustments based on market conditions.
    """

    def __init__(self, total_api_budget: int = 400, max_intensive_symbols: int = 2):
        """
        Initialize the symbol coordinator.

        Args:
            total_api_budget: Total API calls per hour across all symbols
            max_intensive_symbols: Maximum symbols allowed in intensive monitoring
        """
        self.total_api_budget = total_api_budget
        self.max_intensive_symbols = max_intensive_symbols

        # State management
        self.symbol_states: Dict[str, SymbolState] = {}
        self.coordination_lock = threading.RLock()

        # Resource tracking
        self.api_calls_used: int = 0
        self.last_api_reset: datetime = datetime.now()

        # Priority queues for processing
        self.high_priority_symbols: Set[str] = set()
        self.processing_queue: List[Tuple[str, datetime]] = []

        # Performance tracking
        self.coordination_metrics: Dict[str, Any] = {
            'total_symbols_managed': 0,
            'intensive_symbols_count': 0,
            'active_symbols_count': 0,
            'passive_symbols_count': 0,
            'total_api_calls_hour': 0,
            'average_processing_time': 0.0,
            'errors_last_hour': 0
        }

        # Setup logging
        self.logger = logging.getLogger("symbol_coordinator")
        self.logger.setLevel(logging.INFO)

        self.logger.info("SymbolCoordinator initialized")
        self.logger.info(f"Total API budget: {total_api_budget}/hour")
        self.logger.info(f"Max intensive symbols: {max_intensive_symbols}")

    def add_symbol(self, symbol: str, config: Optional[SymbolMonitoringConfig] = None) -> bool:
        """
        Add a symbol to coordination management.

        Args:
            symbol: Symbol to add (e.g., 'AAPL')
            config: Optional custom configuration

        Returns:
            True if successfully added, False otherwise
        """
        try:
            with self.coordination_lock:
                if symbol in self.symbol_states:
                    self.logger.warning(f"Symbol {symbol} already exists, updating config")

                # Create default config if none provided
                if config is None:
                    config = SymbolMonitoringConfig(
                        symbol=symbol,
                        monitoring_intensity=MonitoringIntensity.ACTIVE,
                        update_frequency=300,  # 5 minutes
                        api_call_budget=80
                    )

                # Create resource budget based on symbol priority
                resource_budget = self._create_resource_budget_for_symbol(config)

                # Create symbol state
                self.symbol_states[symbol] = SymbolState(
                    symbol=symbol,
                    config=config,
                    resource_budget=resource_budget
                )

                # Update metrics
                self._update_coordination_metrics()

                self.logger.info(f"Added symbol {symbol} with intensity {config.monitoring_intensity.value}")
                return True

        except Exception as e:
            self.logger.error(f"Failed to add symbol {symbol}: {e}")
            return False

    def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol from coordination management.

        Args:
            symbol: Symbol to remove

        Returns:
            True if successfully removed, False otherwise
        """
        try:
            with self.coordination_lock:
                if symbol not in self.symbol_states:
                    self.logger.warning(f"Symbol {symbol} not found for removal")
                    return False

                # Remove from priority tracking
                self.high_priority_symbols.discard(symbol)

                # Remove from processing queue
                self.processing_queue = [
                    (s, t) for s, t in self.processing_queue if s != symbol
                ]

                # Remove symbol state
                del self.symbol_states[symbol]

                # Update metrics
                self._update_coordination_metrics()

                self.logger.info(f"Removed symbol {symbol}")
                return True

        except Exception as e:
            self.logger.error(f"Failed to remove symbol {symbol}: {e}")
            return False

    def get_next_symbol_to_process(self) -> Optional[str]:
        """
        Get the next symbol that needs processing based on priority and timing.

        Returns:
            Symbol string if one needs processing, None otherwise
        """
        try:
            with self.coordination_lock:
                current_time = datetime.now()
                candidates = []

                # Check all symbols for processing needs
                for symbol, state in self.symbol_states.items():
                    if self._should_process_symbol(symbol, state, current_time):
                        priority_score = self._calculate_priority_score(state)
                        candidates.append((symbol, priority_score))

                # Sort by priority score (higher = more urgent)
                candidates.sort(key=lambda x: x[1], reverse=True)

                # Return highest priority symbol
                if candidates:
                    symbol = candidates[0][0]
                    self.logger.debug(f"Next symbol to process: {symbol}")
                    return symbol

                return None

        except Exception as e:
            self.logger.error(f"Error getting next symbol to process: {e}")
            return None

    def update_symbol_market_data(self, symbol: str, price: float,
                                volume: int, price_change_pct: float) -> None:
        """
        Update market data for a symbol and adjust monitoring intensity if needed.

        Args:
            symbol: Symbol to update
            price: Current price
            volume: Current volume
            price_change_pct: Price change percentage
        """
        try:
            with self.coordination_lock:
                if symbol not in self.symbol_states:
                    self.logger.warning(f"Cannot update market data for unknown symbol {symbol}")
                    return

                state = self.symbol_states[symbol]

                # Update market data
                state.last_price = price
                state.volume = volume
                state.price_change_pct = price_change_pct
                state.last_update = datetime.now()

                # Calculate volatility (simple rolling average)
                if state.price_change_pct is not None:
                    if state.volatility is None:
                        state.volatility = abs(price_change_pct)
                    else:
                        state.volatility = (state.volatility * 0.9 +
                                          abs(price_change_pct) * 0.1)

                # Check if intensity adjustment is needed
                self._check_intensity_adjustment(symbol, state)

                # Update health status
                state.update_health_status()

                self.logger.debug(f"Updated market data for {symbol}: "
                                f"price={price}, change={price_change_pct:.2f}%")

        except Exception as e:
            self.logger.error(f"Error updating market data for {symbol}: {e}")

    def record_processing_result(self, symbol: str, success: bool,
                               processing_time: float, error_message: str = None) -> None:
        """
        Record the result of symbol processing.

        Args:
            symbol: Symbol that was processed
            success: Whether processing was successful
            processing_time: Time taken for processing
            error_message: Error message if processing failed
        """
        try:
            with self.coordination_lock:
                if symbol not in self.symbol_states:
                    self.logger.warning(f"Cannot record result for unknown symbol {symbol}")
                    return

                state = self.symbol_states[symbol]

                if success:
                    state.record_successful_cycle(processing_time)
                    self.logger.debug(f"Recorded successful processing for {symbol}: "
                                    f"{processing_time:.2f}s")
                else:
                    state.record_error(error_message or "Unknown error")
                    self.logger.warning(f"Recorded error for {symbol}: {error_message}")

                # Update coordination metrics
                self._update_coordination_metrics()

        except Exception as e:
            self.logger.error(f"Error recording processing result for {symbol}: {e}")

    def get_symbol_resource_allocation(self, symbol: str) -> Optional[SymbolResourceBudget]:
        """
        Get current resource allocation for a symbol.

        Args:
            symbol: Symbol to get allocation for

        Returns:
            Resource budget if symbol exists, None otherwise
        """
        with self.coordination_lock:
            if symbol in self.symbol_states:
                return self.symbol_states[symbol].resource_budget
            return None

    def can_make_api_call(self, symbol: str) -> bool:
        """
        Check if a symbol can make an API call within resource limits.

        Args:
            symbol: Symbol requesting API call

        Returns:
            True if call is allowed, False otherwise
        """
        try:
            with self.coordination_lock:
                # Check global budget
                if self.api_calls_used >= self.total_api_budget:
                    return False

                # Check symbol-specific budget
                if symbol in self.symbol_states:
                    state = self.symbol_states[symbol]
                    return state.resource_budget.can_make_api_call()

                return False

        except Exception as e:
            self.logger.error(f"Error checking API call permission for {symbol}: {e}")
            return False

    def record_api_call(self, symbol: str) -> None:
        """
        Record an API call for a symbol.

        Args:
            symbol: Symbol that made the API call
        """
        try:
            with self.coordination_lock:
                self.api_calls_used += 1

                if symbol in self.symbol_states:
                    self.symbol_states[symbol].resource_budget.record_api_call()

                self.logger.debug(f"Recorded API call for {symbol}")

        except Exception as e:
            self.logger.error(f"Error recording API call for {symbol}: {e}")

    def get_coordination_status(self) -> Dict[str, Any]:
        """
        Get current coordination status and metrics.

        Returns:
            Dictionary with status information
        """
        try:
            with self.coordination_lock:
                symbol_status = {}
                for symbol, state in self.symbol_states.items():
                    symbol_status[symbol] = {
                        'health': state.health_status.value,
                        'intensity': state.config.monitoring_intensity.value,
                        'success_rate': state.get_success_rate(),
                        'api_calls_used': state.resource_budget.api_calls_used,
                        'last_update': state.last_update.isoformat() if state.last_update else None,
                        'position': state.current_position,
                        'volatility': state.volatility
                    }

                # Debug the type error
                try:
                    api_budget_remaining = self.total_api_budget - self.api_calls_used
                except TypeError as e:
                    self.logger.error(f"Type error in budget calculation: total_api_budget={type(self.total_api_budget)}:{self.total_api_budget}, api_calls_used={type(self.api_calls_used)}:{self.api_calls_used}")
                    api_budget_remaining = 0

                return {
                    'total_symbols': len(self.symbol_states),
                    'api_calls_used': self.api_calls_used,
                    'api_budget_remaining': api_budget_remaining,
                    'intensive_symbols': len([s for s in self.symbol_states.values()
                                            if s.config.monitoring_intensity == MonitoringIntensity.INTENSIVE]),
                    'symbols': symbol_status,
                    'metrics': self.coordination_metrics
                }

        except Exception as e:
            self.logger.error(f"Error getting coordination status: {e}")
            return {'error': str(e)}

    def reset_hourly_counters(self) -> None:
        """Reset hourly API usage counters"""
        try:
            with self.coordination_lock:
                self.api_calls_used = 0
                self.last_api_reset = datetime.now()

                for state in self.symbol_states.values():
                    state.resource_budget.reset_hourly_usage()

                self.logger.info("Reset hourly API usage counters")

        except Exception as e:
            self.logger.error(f"Error resetting hourly counters: {e}")

    def _should_process_symbol(self, symbol: str, state: SymbolState,
                             current_time: datetime) -> bool:
        """Check if a symbol should be processed now"""
        # Check if enough time has passed since last update
        if state.last_update:
            time_since_update = (current_time - state.last_update).total_seconds()
            if time_since_update < state.config.update_frequency:
                return False

        # Check if symbol is healthy enough to process
        if state.health_status == SymbolHealth.SUSPENDED:
            return False

        # Check if we're in recovery wait period after errors
        if (state.consecutive_errors > 0 and state.last_error_time and
            (current_time - state.last_error_time).total_seconds() <
            state.config.recovery_wait_time):
            return False

        return True

    def _calculate_priority_score(self, state: SymbolState) -> float:
        """Calculate priority score for symbol processing"""
        score = 0.0

        # Base score by intensity
        if state.config.monitoring_intensity == MonitoringIntensity.INTENSIVE:
            score += 100
        elif state.config.monitoring_intensity == MonitoringIntensity.ACTIVE:
            score += 50
        else:  # PASSIVE
            score += 10

        # Increase score for high volatility
        if state.volatility:
            score += state.volatility * 1000

        # Increase score for large positions
        score += abs(state.current_position) * 200

        # Increase score for significant price changes
        if state.price_change_pct:
            score += abs(state.price_change_pct) * 500

        # Decrease score for recent errors (temporary penalty)
        score -= state.consecutive_errors * 20

        return score

    def _check_intensity_adjustment(self, symbol: str, state: SymbolState) -> None:
        """Check if monitoring intensity should be adjusted"""
        current_intensity = state.config.monitoring_intensity

        # Check if should increase intensity
        if (current_intensity != MonitoringIntensity.INTENSIVE and
            state.should_increase_intensity()):

            # Only allow increase if we have intensive slots available
            intensive_count = len([s for s in self.symbol_states.values()
                                 if s.config.monitoring_intensity == MonitoringIntensity.INTENSIVE])

            if intensive_count < self.max_intensive_symbols:
                self._adjust_monitoring_intensity(symbol, MonitoringIntensity.INTENSIVE)

        # Check if should decrease intensity
        elif (current_intensity != MonitoringIntensity.PASSIVE and
              state.should_decrease_intensity()):

            if current_intensity == MonitoringIntensity.INTENSIVE:
                self._adjust_monitoring_intensity(symbol, MonitoringIntensity.ACTIVE)
            else:  # ACTIVE -> PASSIVE
                self._adjust_monitoring_intensity(symbol, MonitoringIntensity.PASSIVE)

    def _adjust_monitoring_intensity(self, symbol: str, new_intensity: MonitoringIntensity) -> None:
        """Adjust monitoring intensity for a symbol"""
        try:
            state = self.symbol_states[symbol]
            old_intensity = state.config.monitoring_intensity

            state.config.monitoring_intensity = new_intensity

            # Adjust update frequency based on intensity
            if new_intensity == MonitoringIntensity.INTENSIVE:
                state.config.update_frequency = 60   # 1 minute
            elif new_intensity == MonitoringIntensity.ACTIVE:
                state.config.update_frequency = 300  # 5 minutes
            else:  # PASSIVE
                state.config.update_frequency = 900  # 15 minutes

            # Reallocate resources
            state.resource_budget = self._create_resource_budget_for_symbol(state.config)

            self.logger.info(f"Adjusted {symbol} intensity: {old_intensity.value} -> {new_intensity.value}")

        except Exception as e:
            self.logger.error(f"Error adjusting intensity for {symbol}: {e}")

    def _create_resource_budget_for_symbol(self, config: SymbolMonitoringConfig) -> SymbolResourceBudget:
        """Create resource budget based on symbol configuration"""
        # Base allocations
        if config.monitoring_intensity == MonitoringIntensity.INTENSIVE:
            api_budget = 120
            cpu_pct = 25.0
            memory_mb = 200.0
        elif config.monitoring_intensity == MonitoringIntensity.ACTIVE:
            api_budget = 80
            cpu_pct = 16.0
            memory_mb = 160.0
        else:  # PASSIVE
            api_budget = 40
            cpu_pct = 8.0
            memory_mb = 100.0

        return SymbolResourceBudget(
            api_calls_per_hour=api_budget,
            cpu_allocation_pct=cpu_pct,
            memory_allocation_mb=memory_mb,
            max_processing_time=config.max_decision_time
        )

    def _update_coordination_metrics(self) -> None:
        """Update coordination performance metrics"""
        try:
            total_symbols = len(self.symbol_states)
            intensive_count = len([s for s in self.symbol_states.values()
                                 if s.config.monitoring_intensity == MonitoringIntensity.INTENSIVE])
            active_count = len([s for s in self.symbol_states.values()
                              if s.config.monitoring_intensity == MonitoringIntensity.ACTIVE])
            passive_count = total_symbols - intensive_count - active_count

            self.coordination_metrics.update({
                'total_symbols_managed': total_symbols,
                'intensive_symbols_count': intensive_count,
                'active_symbols_count': active_count,
                'passive_symbols_count': passive_count,
                'total_api_calls_hour': self.api_calls_used
            })

        except Exception as e:
            self.logger.error(f"Error updating coordination metrics: {e}")

    def update_symbol_position(self, symbol: str, position_data: Dict[str, Any]) -> None:
        """
        Update position information for a symbol.

        Args:
            symbol: Symbol to update
            position_data: Position details including shares, value, etc.
        """
        try:
            with self.coordination_lock:
                if symbol not in self.symbol_states:
                    self.logger.warning(f"Symbol {symbol} not in coordinator - auto-adding it")
                    # Auto-add the missing symbol with default settings
                    self.add_symbol(symbol)
                    
                    if symbol not in self.symbol_states:
                        self.logger.error(f"Failed to auto-add symbol {symbol} to coordinator")
                        return

                state = self.symbol_states[symbol]

                # Update position information
                state.current_position = position_data.get('shares', 0.0)
                state.position_value = position_data.get('market_value', 0.0)
                state.unrealized_pnl = position_data.get('unrealized_pl', 0.0)

                # Update last update time
                state.last_update = datetime.now()

                # Check if intensity adjustment is needed based on position size
                self._check_intensity_adjustment(symbol, state)

                # Update health status
                state.update_health_status()

                self.logger.info(f"Updated position for {symbol}: "
                               f"shares={state.current_position}, "
                               f"value=${state.position_value:,.2f}, "
                               f"unrealized_pnl=${state.unrealized_pnl:+,.2f}")

        except Exception as e:
            self.logger.error(f"Error updating position for {symbol}: {e}")

    def sync_positions_from_database(self, db_manager) -> None:
        """
        Sync all positions from database to symbol coordinator.
        If no database holdings exist, ensure configured symbols are still added for monitoring.

        Args:
            db_manager: Database manager to get holdings from
        """
        try:
            self.logger.info("=== SYNC POSITIONS DEBUG START ===")
            self.logger.info("Syncing positions from database to symbol coordinator...")
            
            # DEBUG: Show current symbol_states before sync
            self.logger.info(f"BEFORE SYNC: Symbol states count = {len(self.symbol_states)}")
            for symbol, state in self.symbol_states.items():
                is_active = state.current_position > 0
                self.logger.info(f"  {symbol}: position={state.current_position}, active={is_active}")

            # Get all active holdings from database
            self.logger.info("Getting holdings from database...")
            holdings = db_manager.get_holdings(active_only=True) if hasattr(db_manager, 'get_holdings') else []
            self.logger.info(f"Retrieved {len(holdings)} holdings from database")

            synced_symbols = set()

            # Process database holdings
            for holding in holdings:
                symbol = holding['symbol']

                # Handle different column name mappings
                if 'net_quantity' in holding:
                    shares = float(holding['net_quantity'])
                    total_invested = float(holding['total_invested'])
                    avg_cost_basis = float(holding['avg_cost_basis'])
                elif 'quantity' in holding:
                    shares = float(holding['quantity'])
                    total_invested = float(holding['total_invested'])
                    avg_cost_basis = float(holding['avg_cost'])
                else:
                    self.logger.warning(f"Unknown holding format for {symbol}: {holding}")
                    continue

                # Only update if we have shares (active position)
                if shares != 0:
                    position_data = {
                        'shares': shares,
                        'market_value': total_invested,  # Approximate market value
                        'unrealized_pl': 0.0,  # Will be updated when we get current prices
                        'avg_entry_price': avg_cost_basis
                    }

                    self.update_symbol_position(symbol, position_data)
                    self.logger.info(f"Synced position for {symbol}: {shares} shares")
                    synced_symbols.add(symbol)
                else:
                    # Clear position if no shares
                    self.update_symbol_position(symbol, {'shares': 0.0, 'market_value': 0.0, 'unrealized_pl': 0.0})
                    synced_symbols.add(symbol)

            # CRITICAL FIX: If no database holdings, ensure configured symbols are still added as monitoring candidates
            if len(holdings) == 0:
                self.logger.warning("NO DATABASE HOLDINGS FOUND - Adding configured symbols for monitoring")
                
                # Add symbols that are already in symbol_states but not synced from database
                for symbol in self.symbol_states.keys():
                    if symbol not in synced_symbols:
                        # Add as monitoring candidate with zero position but ACTIVE intensity
                        position_data = {
                            'shares': 0.0,
                            'market_value': 0.0,
                            'unrealized_pl': 0.0,
                            'avg_entry_price': 0.0
                        }
                        self.update_symbol_position(symbol, position_data)
                        
                        # IMPORTANT: Set as ACTIVE intensity for monitoring even without position
                        if symbol in self.symbol_states:
                            self.symbol_states[symbol].config.monitoring_intensity = MonitoringIntensity.ACTIVE
                            self.logger.info(f"FIXED: {symbol} set to ACTIVE monitoring (no position but configured for trading)")
                        
                        synced_symbols.add(symbol)

            self.logger.info(f"Position sync completed - {len(holdings)} holdings from DB, {len(synced_symbols)} total symbols synced")
            
            # FINAL STATUS DEBUG: Show what the debug monitor will see
            final_active_count = 0
            for symbol, state in self.symbol_states.items():
                position = state.current_position
                intensity = state.config.monitoring_intensity.value
                if position > 0:
                    final_active_count += 1
                    self.logger.info(f"ACTIVE SYMBOL: {symbol} - position={position}, intensity={intensity}")
                else:
                    self.logger.info(f"MONITORING SYMBOL: {symbol} - position={position}, intensity={intensity}")
            
            if final_active_count == 0:
                self.logger.warning("ISSUE: Still no active symbols with positions > 0")
                self.logger.info("NOTE: Debug monitor shows 'Active=0' because no symbols have actual stock positions")
                self.logger.info("SOLUTION: Either seed the database with test positions or modify activity definition")
            else:
                self.logger.info(f"SUCCESS: {final_active_count} symbols now have active positions")
            
            self.logger.info("=== SYNC POSITIONS DEBUG END ===")

        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in sync_positions_from_database: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            traceback.print_exc()
