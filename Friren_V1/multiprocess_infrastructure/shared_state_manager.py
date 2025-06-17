"""
multiprocess_infrastructure/shared_state_manager.py

Thread-safe shared state management for multiprocess trading system.
Provides centralized access to market data, positions, and system state.
"""

import multiprocessing as mp
import threading
import time
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from collections import defaultdict
import logging


@dataclass
class SharedStateSnapshot:
    """Snapshot of current shared state"""
    websocket_prices: Dict[str, float]
    active_positions: Dict[str, Dict[str, Any]]
    sentiment_scores: Dict[str, float]
    market_regime: str
    strategy_signals: Dict[str, Dict[str, Any]]
    system_status: Dict[str, Any]
    last_updated: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)


class SharedStateManager:
    """
    Thread-safe shared state manager for multiprocess trading system

    Manages:
    - Real-time price data from WebSocket
    - Current portfolio positions
    - Market sentiment scores
    - Market regime classification
    - Latest strategy signals
    - System health status

    All reads/writes are thread-safe and optimized for high-frequency access.
    """

    def __init__(self):
        self.logger = logging.getLogger("shared_state_manager")

        # Thread safety
        self._lock = threading.RLock()

        # Shared state components
        self._websocket_prices = {}      # {symbol: price}
        self._active_positions = {}      # {symbol: position_data}
        self._sentiment_scores = {}      # {symbol: sentiment_score}
        self._market_regime = "UNKNOWN"  # Current market regime
        self._strategy_signals = {}      # {strategy_name: {symbol: signal_data}}
        self._system_status = {}         # System health and status

        # Metadata
        self._last_price_update = None
        self._last_position_update = None
        self._last_sentiment_update = None
        self._last_regime_update = None

        # Change tracking for notifications
        self._change_callbacks = defaultdict(list)

        # Performance monitoring
        self._read_count = 0
        self._write_count = 0
        self._last_stats_reset = time.time()

        self.logger.info("SharedStateManager initialized")

    # WebSocket Price Management

    def update_prices(self, price_data: Dict[str, float]):
        """
        Update real-time price data from WebSocket

        Args:
            price_data: Dictionary of {symbol: current_price}
        """
        with self._lock:
            self._websocket_prices.update(price_data)
            self._last_price_update = datetime.now()
            self._write_count += 1

            # Trigger callbacks for price changes
            self._trigger_callbacks('price_update', price_data)

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for symbol"""
        with self._lock:
            self._read_count += 1
            return self._websocket_prices.get(symbol)

    def get_all_prices(self) -> Dict[str, float]:
        """Get all current prices"""
        with self._lock:
            self._read_count += 1
            return self._websocket_prices.copy()

    def get_price_age(self, symbol: str) -> Optional[float]:
        """Get age of price data in seconds"""
        if self._last_price_update and symbol in self._websocket_prices:
            return (datetime.now() - self._last_price_update).total_seconds()
        return None

    # Position Management

    def update_position(self, symbol: str, position_data: Dict[str, Any]):
        """
        Update position information

        Args:
            symbol: Stock symbol
            position_data: Position details (shares, entry_price, etc.)
        """
        with self._lock:
            self._active_positions[symbol] = {
                **position_data,
                'last_updated': datetime.now()
            }
            self._last_position_update = datetime.now()
            self._write_count += 1

            self._trigger_callbacks('position_update', {symbol: position_data})

    def remove_position(self, symbol: str):
        """Remove position (when closed)"""
        with self._lock:
            if symbol in self._active_positions:
                removed = self._active_positions.pop(symbol)
                self._last_position_update = datetime.now()
                self._write_count += 1

                self._trigger_callbacks('position_removed', {symbol: removed})

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get position data for symbol"""
        with self._lock:
            self._read_count += 1
            return self._active_positions.get(symbol)

    def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active positions"""
        with self._lock:
            self._read_count += 1
            return self._active_positions.copy()

    def get_portfolio_value(self) -> float:
        """Calculate current portfolio value using latest prices"""
        with self._lock:
            total_value = 0.0

            for symbol, position in self._active_positions.items():
                shares = position.get('shares', 0)
                current_price = self._websocket_prices.get(symbol)

                if current_price:
                    total_value += shares * current_price
                else:
                    # Fallback to entry price if no current price
                    entry_price = position.get('entry_price', 0)
                    total_value += shares * entry_price

            self._read_count += 1
            return total_value

    # Sentiment Management

    def update_sentiment(self, symbol: str, sentiment_score: float):
        """Update sentiment score for symbol"""
        with self._lock:
            self._sentiment_scores[symbol] = sentiment_score
            self._last_sentiment_update = datetime.now()
            self._write_count += 1

            self._trigger_callbacks('sentiment_update', {symbol: sentiment_score})

    def update_sentiments(self, sentiment_data: Dict[str, float]):
        """Batch update sentiment scores"""
        with self._lock:
            self._sentiment_scores.update(sentiment_data)
            self._last_sentiment_update = datetime.now()
            self._write_count += 1

            self._trigger_callbacks('sentiment_update', sentiment_data)

    def get_sentiment(self, symbol: str) -> Optional[float]:
        """Get sentiment score for symbol"""
        with self._lock:
            self._read_count += 1
            return self._sentiment_scores.get(symbol)

    def get_all_sentiments(self) -> Dict[str, float]:
        """Get all sentiment scores"""
        with self._lock:
            self._read_count += 1
            return self._sentiment_scores.copy()

    # Market Regime Management

    def update_market_regime(self, regime: str, confidence: float = 0.0, metadata: Optional[Dict] = None):
        """Update current market regime"""
        with self._lock:
            old_regime = self._market_regime
            self._market_regime = regime
            self._last_regime_update = datetime.now()
            self._write_count += 1

            regime_data = {
                'old_regime': old_regime,
                'new_regime': regime,
                'confidence': confidence,
                'metadata': metadata or {}
            }

            self._trigger_callbacks('regime_change', regime_data)

    def get_market_regime(self) -> str:
        """Get current market regime"""
        with self._lock:
            self._read_count += 1
            return self._market_regime

    # Strategy Signal Management

    def update_strategy_signal(self, strategy_name: str, symbol: str, signal_data: Dict[str, Any]):
        """Update signal from a specific strategy"""
        with self._lock:
            if strategy_name not in self._strategy_signals:
                self._strategy_signals[strategy_name] = {}

            self._strategy_signals[strategy_name][symbol] = {
                **signal_data,
                'timestamp': datetime.now()
            }
            self._write_count += 1

            callback_data = {
                'strategy': strategy_name,
                'symbol': symbol,
                'signal': signal_data
            }
            self._trigger_callbacks('strategy_signal', callback_data)

    def get_strategy_signals(self, strategy_name: str) -> Dict[str, Dict[str, Any]]:
        """Get all signals from a specific strategy"""
        with self._lock:
            self._read_count += 1
            return self._strategy_signals.get(strategy_name, {}).copy()

    def get_symbol_signals(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        """Get all strategy signals for a specific symbol"""
        with self._lock:
            symbol_signals = {}
            for strategy_name, signals in self._strategy_signals.items():
                if symbol in signals:
                    symbol_signals[strategy_name] = signals[symbol]

            self._read_count += 1
            return symbol_signals

    def get_all_strategy_signals(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Get all strategy signals"""
        with self._lock:
            self._read_count += 1
            return {
                strategy: signals.copy()
                for strategy, signals in self._strategy_signals.items()
            }

    def clear_old_signals(self, max_age_minutes: int = 10):
        """Remove signals older than specified age"""
        with self._lock:
            cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)

            for strategy_name in list(self._strategy_signals.keys()):
                for symbol in list(self._strategy_signals[strategy_name].keys()):
                    signal_time = self._strategy_signals[strategy_name][symbol].get('timestamp')
                    if signal_time and signal_time < cutoff_time:
                        del self._strategy_signals[strategy_name][symbol]

                # Remove empty strategy entries
                if not self._strategy_signals[strategy_name]:
                    del self._strategy_signals[strategy_name]

    # System Status Management

    def update_system_status(self, component: str, status_data: Dict[str, Any]):
        """Update system status for a component"""
        with self._lock:
            self._system_status[component] = {
                **status_data,
                'last_updated': datetime.now()
            }
            self._write_count += 1

            self._trigger_callbacks('system_status', {component: status_data})

    def get_system_status(self, component: Optional[str] = None) -> Dict[str, Any]:
        """Get system status for component or all components"""
        with self._lock:
            self._read_count += 1
            if component:
                return self._system_status.get(component, {})
            return self._system_status.copy()

    # Snapshot and State Management

    def get_full_snapshot(self) -> SharedStateSnapshot:
        """Get complete snapshot of current state"""
        with self._lock:
            self._read_count += 1
            return SharedStateSnapshot(
                websocket_prices=self._websocket_prices.copy(),
                active_positions=self._active_positions.copy(),
                sentiment_scores=self._sentiment_scores.copy(),
                market_regime=self._market_regime,
                strategy_signals=self._get_strategy_signals_copy(),
                system_status=self._system_status.copy(),
                last_updated=datetime.now()
            )

    def _get_strategy_signals_copy(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Get deep copy of strategy signals"""
        return {
            strategy: {
                symbol: signal.copy()
                for symbol, signal in signals.items()
            }
            for strategy, signals in self._strategy_signals.items()
        }

    def export_state(self, file_path: str):
        """Export current state to JSON file"""
        try:
            snapshot = self.get_full_snapshot()

            # Convert datetime objects to strings for JSON serialization
            data = snapshot.to_dict()
            data['last_updated'] = data['last_updated'].isoformat()

            # Convert other datetime objects in nested structures
            for positions in data['active_positions'].values():
                if 'last_updated' in positions:
                    positions['last_updated'] = positions['last_updated'].isoformat()

            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)

            self.logger.info(f"State exported to {file_path}")

        except Exception as e:
            self.logger.error(f"Error exporting state: {e}")

    # Callback Management

    def register_callback(self, event_type: str, callback_func):
        """Register callback for state changes"""
        self._change_callbacks[event_type].append(callback_func)
        self.logger.debug(f"Registered callback for {event_type}")

    def _trigger_callbacks(self, event_type: str, data: Dict[str, Any]):
        """Trigger callbacks for event type"""
        try:
            for callback in self._change_callbacks[event_type]:
                callback(data)
        except Exception as e:
            self.logger.error(f"Error in callback for {event_type}: {e}")

    # Performance and Monitoring

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        with self._lock:
            uptime_seconds = time.time() - self._last_stats_reset

            stats = {
                'uptime_seconds': uptime_seconds,
                'total_reads': self._read_count,
                'total_writes': self._write_count,
                'reads_per_second': self._read_count / uptime_seconds if uptime_seconds > 0 else 0,
                'writes_per_second': self._write_count / uptime_seconds if uptime_seconds > 0 else 0,
                'price_symbols_count': len(self._websocket_prices),
                'active_positions_count': len(self._active_positions),
                'sentiment_symbols_count': len(self._sentiment_scores),
                'strategy_count': len(self._strategy_signals),
                'last_price_update': self._last_price_update.isoformat() if self._last_price_update else None,
                'last_position_update': self._last_position_update.isoformat() if self._last_position_update else None,
                'last_sentiment_update': self._last_sentiment_update.isoformat() if self._last_sentiment_update else None,
                'last_regime_update': self._last_regime_update.isoformat() if self._last_regime_update else None
            }

            return stats

    def reset_performance_stats(self):
        """Reset performance counters"""
        with self._lock:
            self._read_count = 0
            self._write_count = 0
            self._last_stats_reset = time.time()
            self.logger.info("Performance stats reset")

    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check of shared state"""
        with self._lock:
            now = datetime.now()

            # Check data freshness
            price_staleness = (now - self._last_price_update).total_seconds() if self._last_price_update else float('inf')
            position_staleness = (now - self._last_position_update).total_seconds() if self._last_position_update else float('inf')

            # Determine health status
            health_status = "healthy"
            warnings = []

            if price_staleness > 60:  # Prices older than 1 minute
                health_status = "warning"
                warnings.append(f"Price data stale ({price_staleness:.1f}s)")

            if len(self._websocket_prices) == 0:
                health_status = "critical"
                warnings.append("No price data available")

            if price_staleness > 300:  # Prices older than 5 minutes
                health_status = "critical"
                warnings.append("Price data critically stale")

            return {
                'status': health_status,
                'warnings': warnings,
                'price_staleness_seconds': price_staleness,
                'position_staleness_seconds': position_staleness,
                'data_counts': {
                    'prices': len(self._websocket_prices),
                    'positions': len(self._active_positions),
                    'sentiments': len(self._sentiment_scores),
                    'strategies': len(self._strategy_signals)
                },
                'current_regime': self._market_regime,
                'check_time': now.isoformat()
            }

    def cleanup(self):
        """Clean up resources"""
        with self._lock:
            self.logger.info("Cleaning up SharedStateManager")

            # Clear all data
            self._websocket_prices.clear()
            self._active_positions.clear()
            self._sentiment_scores.clear()
            self._strategy_signals.clear()
            self._system_status.clear()

            # Clear callbacks
            self._change_callbacks.clear()

            self.logger.info("SharedStateManager cleanup complete")
