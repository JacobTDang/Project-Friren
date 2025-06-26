"""
Watchlist Manager for Trading System

Manages the comprehensive watchlist including:
- Current holdings monitoring
- Potential opportunities
- Entry/exit criteria tracking
- Priority-based monitoring

Integrates with portfolio manager and decision engine for
intelligent watchlist-based trading decisions.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum

@dataclass
class WatchlistEntry:
    """Watchlist entry data structure"""
    symbol: str
    status: str
    priority: int
    target_entry_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    max_position_size_pct: float = 10.0
    sentiment_score: Optional[float] = None
    technical_score: Optional[float] = None
    risk_score: Optional[float] = None
    notes: str = ""
    tags: List[str] = None
    sector: str = ""
    is_holding: bool = False
    current_position_value: float = 0.0
    days_on_watchlist: int = 0

    def __post_init__(self):
        if self.tags is None:
            self.tags = []

class WatchlistStatus(Enum):
    """Watchlist status categories"""
    HOLDING = "HOLDING"
    WATCHING = "WATCHING"
    ANALYZING = "ANALYZING"
    BLACKLIST = "BLACKLIST"
    SOLD = "SOLD"

class WatchlistManager:
    """
    Comprehensive Watchlist Manager

    Handles watchlist operations, portfolio integration,
    and intelligent monitoring for trading opportunities.
    """

    def __init__(self, db_manager=None, position_sizer=None):
        """Initialize watchlist manager"""
        self.logger = logging.getLogger("watchlist_manager")
        self.db_manager = db_manager
        self.position_sizer = position_sizer

        # In-memory cache for performance
        self._watchlist_cache: Dict[str, WatchlistEntry] = {}
        self._cache_last_updated = None
        self._cache_duration = 60  # seconds

        self.logger.info("WatchlistManager initialized")

    def initialize_from_portfolio(self, symbols: List[str] = None) -> Dict[str, Any]:
        """
        Initialize watchlist from current portfolio and symbol list

        Args:
            symbols: List of symbols to monitor

        Returns:
            Dict with initialization results
        """
        try:
            results = {
                'holdings_added': 0,
                'new_symbols_added': 0,
                'total_watchlist_size': 0,
                'errors': []
            }

            # Load current holdings from database
            if self.db_manager:
                try:
                    holdings = self.db_manager.get_holdings(active_only=True)

                    for holding in holdings:
                        symbol = holding['symbol']
                        entry = WatchlistEntry(
                            symbol=symbol,
                            status=WatchlistStatus.HOLDING.value,
                            priority=8,  # High priority for holdings
                            is_holding=True,
                            current_position_value=float(holding.get('total_invested', 0)),
                            notes=f"Existing position: {holding.get('net_quantity', 0)} shares"
                        )
                        self._watchlist_cache[symbol] = entry
                        results['holdings_added'] += 1

                    self.logger.info(f"Loaded {results['holdings_added']} existing holdings to watchlist")

                except Exception as e:
                    self.logger.error(f"Error loading holdings: {e}")
                    results['errors'].append(f"Holdings load error: {e}")

            # Add new symbols from parameter list
            if symbols:
                for symbol in symbols:
                    if symbol not in self._watchlist_cache:
                        entry = WatchlistEntry(
                            symbol=symbol,
                            status=WatchlistStatus.WATCHING.value,
                            priority=5,  # Medium priority for new symbols
                            max_position_size_pct=10.0,
                            notes="Added from symbol list"
                        )
                        self._watchlist_cache[symbol] = entry
                        results['new_symbols_added'] += 1

                self.logger.info(f"Added {results['new_symbols_added']} new symbols to watchlist")

            results['total_watchlist_size'] = len(self._watchlist_cache)
            self._cache_last_updated = datetime.now()

            return results

        except Exception as e:
            self.logger.error(f"Error initializing watchlist: {e}")
            return {'error': str(e)}

    def get_watchlist(self, status_filter: Optional[str] = None,
                     priority_min: int = 1) -> List[WatchlistEntry]:
        """
        Get filtered watchlist entries

        Args:
            status_filter: Filter by status (HOLDING, WATCHING, etc.)
            priority_min: Minimum priority level

        Returns:
            List of watchlist entries
        """
        try:
            self._refresh_cache_if_needed()

            filtered_entries = []
            for entry in self._watchlist_cache.values():
                # Apply filters
                if status_filter and entry.status != status_filter:
                    continue
                if entry.priority < priority_min:
                    continue

                filtered_entries.append(entry)

            # Sort by priority (high to low), then by symbol
            filtered_entries.sort(key=lambda x: (-x.priority, x.symbol))

            return filtered_entries

        except Exception as e:
            self.logger.error(f"Error getting watchlist: {e}")
            return []

    def get_holdings_watchlist(self) -> List[WatchlistEntry]:
        """Get only symbols we're currently holding"""
        return self.get_watchlist(status_filter=WatchlistStatus.HOLDING.value)

    def get_opportunities_watchlist(self, top_n: int = 10) -> List[WatchlistEntry]:
        """Get top opportunities we're watching"""
        opportunities = self.get_watchlist(status_filter=WatchlistStatus.WATCHING.value)
        return opportunities[:top_n]

    def update_entry_analysis(self, symbol: str, sentiment_score: float = None,
                            technical_score: float = None, risk_score: float = None):
        """Update analysis scores for a watchlist entry"""
        try:
            if symbol in self._watchlist_cache:
                entry = self._watchlist_cache[symbol]

                if sentiment_score is not None:
                    entry.sentiment_score = sentiment_score
                if technical_score is not None:
                    entry.technical_score = technical_score
                if risk_score is not None:
                    entry.risk_score = risk_score

                self.logger.debug(f"Updated analysis for {symbol}")
                return True
            else:
                self.logger.warning(f"Symbol {symbol} not found in watchlist")
                return False

        except Exception as e:
            self.logger.error(f"Error updating analysis for {symbol}: {e}")
            return False

    def mark_position_opened(self, symbol: str, position_value: float):
        """Mark that we've opened a position in this symbol"""
        try:
            if symbol in self._watchlist_cache:
                entry = self._watchlist_cache[symbol]
                entry.status = WatchlistStatus.HOLDING.value
                entry.is_holding = True
                entry.current_position_value = position_value
                entry.priority = max(entry.priority, 8)  # Boost priority for holdings

                self.logger.info(f"Marked {symbol} as HOLDING with value ${position_value:,.2f}")
                return True
            else:
                # Add new entry for symbol we just bought
                entry = WatchlistEntry(
                    symbol=symbol,
                    status=WatchlistStatus.HOLDING.value,
                    priority=8,
                    is_holding=True,
                    current_position_value=position_value,
                    notes="Position opened"
                )
                self._watchlist_cache[symbol] = entry
                self.logger.info(f"Added new holding {symbol} to watchlist")
                return True

        except Exception as e:
            self.logger.error(f"Error marking position opened for {symbol}: {e}")
            return False

    def mark_position_closed(self, symbol: str, sold_recently: bool = True):
        """Mark that we've closed a position in this symbol"""
        try:
            if symbol in self._watchlist_cache:
                entry = self._watchlist_cache[symbol]
                entry.is_holding = False
                entry.current_position_value = 0.0

                if sold_recently:
                    entry.status = WatchlistStatus.SOLD.value
                    entry.priority = 6  # Medium-high priority to monitor after sale
                else:
                    entry.status = WatchlistStatus.WATCHING.value
                    entry.priority = 5  # Normal priority

                self.logger.info(f"Marked {symbol} as {'SOLD' if sold_recently else 'WATCHING'}")
                return True
            else:
                self.logger.warning(f"Symbol {symbol} not found in watchlist when closing position")
                return False

        except Exception as e:
            self.logger.error(f"Error marking position closed for {symbol}: {e}")
            return False

    def get_watchlist_summary(self) -> Dict[str, Any]:
        """Get comprehensive watchlist summary"""
        try:
            self._refresh_cache_if_needed()

            summary = {
                'total_symbols': len(self._watchlist_cache),
                'by_status': {},
                'by_priority': {},
                'holdings_value': 0.0,
                'top_opportunities': [],
                'needs_analysis': []
            }

            # Count by status and priority
            for entry in self._watchlist_cache.values():
                # By status
                status = entry.status
                summary['by_status'][status] = summary['by_status'].get(status, 0) + 1

                # By priority
                priority = entry.priority
                summary['by_priority'][priority] = summary['by_priority'].get(priority, 0) + 1

                # Holdings value
                if entry.is_holding:
                    summary['holdings_value'] += entry.current_position_value

                # Top opportunities (high priority watching)
                if entry.status == WatchlistStatus.WATCHING.value and entry.priority >= 7:
                    summary['top_opportunities'].append({
                        'symbol': entry.symbol,
                        'priority': entry.priority,
                        'sentiment_score': entry.sentiment_score
                    })

                # Needs analysis (no recent scores)
                if entry.sentiment_score is None and entry.status != WatchlistStatus.BLACKLIST.value:
                    summary['needs_analysis'].append(entry.symbol)

            return summary

        except Exception as e:
            self.logger.error(f"Error generating watchlist summary: {e}")
            return {'error': str(e)}

    def _refresh_cache_if_needed(self):
        """Refresh cache if it's stale"""
        if (self._cache_last_updated is None or
            (datetime.now() - self._cache_last_updated).total_seconds() > self._cache_duration):

            # In a full implementation, this would reload from database
            # For now, just update the timestamp
            self._cache_last_updated = datetime.now()

    def add_symbol(self, symbol: str, priority: int = 5, notes: str = "",
                  tags: List[str] = None) -> bool:
        """Add a new symbol to watchlist"""
        try:
            if symbol in self._watchlist_cache:
                self.logger.info(f"Symbol {symbol} already in watchlist")
                return True

            entry = WatchlistEntry(
                symbol=symbol,
                status=WatchlistStatus.WATCHING.value,
                priority=priority,
                notes=notes,
                tags=tags or []
            )

            self._watchlist_cache[symbol] = entry
            self.logger.info(f"Added {symbol} to watchlist with priority {priority}")
            return True

        except Exception as e:
            self.logger.error(f"Error adding symbol {symbol}: {e}")
            return False

    def remove_symbol(self, symbol: str) -> bool:
        """Remove symbol from watchlist"""
        try:
            if symbol in self._watchlist_cache:
                del self._watchlist_cache[symbol]
                self.logger.info(f"Removed {symbol} from watchlist")
                return True
            else:
                self.logger.warning(f"Symbol {symbol} not found in watchlist")
                return False

        except Exception as e:
            self.logger.error(f"Error removing symbol {symbol}: {e}")
            return False

def create_watchlist_manager(db_manager=None, position_sizer=None) -> WatchlistManager:
    """Factory function to create watchlist manager"""
    return WatchlistManager(db_manager, position_sizer)
