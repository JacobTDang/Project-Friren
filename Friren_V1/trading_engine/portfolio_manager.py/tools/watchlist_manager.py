"""
portfolio_manager/tools/watchlist_manager.py

Simple watchlist management tool for the trading system orchestrator.
Allows users to manage symbols for monitoring and analysis.
"""

import json
import os
from typing import List, Set, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import logging


@dataclass
class WatchlistEntry:
    """Single watchlist entry with metadata"""
    symbol: str
    added_date: datetime
    priority: str = "normal"  # "high", "normal", "low"
    category: str = "general"  # "tech", "finance", "growth", "dividend", etc.
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'symbol': self.symbol,
            'added_date': self.added_date.isoformat(),
            'priority': self.priority,
            'category': self.category,
            'notes': self.notes
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WatchlistEntry':
        """Create from dictionary"""
        return cls(
            symbol=data['symbol'],
            added_date=datetime.fromisoformat(data['added_date']),
            priority=data.get('priority', 'normal'),
            category=data.get('category', 'general'),
            notes=data.get('notes', '')
        )


class WatchlistManager:
    """
    Simple Watchlist Management Tool

    Features:
    - Add/remove symbols from watchlist
    - Categorize symbols (tech, finance, growth, etc.)
    - Set priority levels (high, normal, low)
    - Persistent storage to JSON file
    - Symbol validation
    - Bulk operations
    - Export capabilities
    """

    def __init__(self, watchlist_file: str = "config/watchlist.json"):
        self.watchlist_file = watchlist_file
        self.logger = logging.getLogger("watchlist_manager")

        # Core watchlist storage
        self._watchlist: Dict[str, WatchlistEntry] = {}

        # Ensure config directory exists
        os.makedirs(os.path.dirname(self.watchlist_file), exist_ok=True)

        # Load existing watchlist
        self._load_watchlist()

        self.logger.info(f"WatchlistManager initialized with {len(self._watchlist)} symbols")

    def add_symbol(self, symbol: str, priority: str = "normal",
                   category: str = "general", notes: str = "") -> bool:
        """
        Add a symbol to the watchlist

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            priority: Priority level ('high', 'normal', 'low')
            category: Category ('tech', 'finance', 'growth', etc.)
            notes: Optional notes about the symbol

        Returns:
            True if added successfully, False if already exists
        """
        symbol = symbol.upper().strip()

        # Basic validation
        if not self._validate_symbol(symbol):
            self.logger.warning(f"Invalid symbol format: {symbol}")
            return False

        if symbol in self._watchlist:
            self.logger.info(f"Symbol {symbol} already in watchlist")
            return False

        # Add to watchlist
        entry = WatchlistEntry(
            symbol=symbol,
            added_date=datetime.now(),
            priority=priority.lower(),
            category=category.lower(),
            notes=notes
        )

        self._watchlist[symbol] = entry
        self._save_watchlist()

        self.logger.info(f"Added {symbol} to watchlist (priority: {priority}, category: {category})")
        return True

    def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol from the watchlist

        Args:
            symbol: Stock symbol to remove

        Returns:
            True if removed successfully, False if not found
        """
        symbol = symbol.upper().strip()

        if symbol not in self._watchlist:
            self.logger.info(f"Symbol {symbol} not found in watchlist")
            return False

        removed_entry = self._watchlist.pop(symbol)
        self._save_watchlist()

        self.logger.info(f"Removed {symbol} from watchlist")
        return True

    def update_symbol(self, symbol: str, priority: Optional[str] = None,
                     category: Optional[str] = None, notes: Optional[str] = None) -> bool:
        """
        Update an existing symbol's metadata

        Args:
            symbol: Stock symbol to update
            priority: New priority level (optional)
            category: New category (optional)
            notes: New notes (optional)

        Returns:
            True if updated successfully, False if not found
        """
        symbol = symbol.upper().strip()

        if symbol not in self._watchlist:
            self.logger.info(f"Symbol {symbol} not found in watchlist")
            return False

        entry = self._watchlist[symbol]

        if priority is not None:
            entry.priority = priority.lower()
        if category is not None:
            entry.category = category.lower()
        if notes is not None:
            entry.notes = notes

        self._save_watchlist()

        self.logger.info(f"Updated {symbol} in watchlist")
        return True

    def get_symbols(self, priority: Optional[str] = None,
                   category: Optional[str] = None) -> List[str]:
        """
        Get list of symbols with optional filtering

        Args:
            priority: Filter by priority level (optional)
            category: Filter by category (optional)

        Returns:
            List of symbol strings
        """
        symbols = []

        for symbol, entry in self._watchlist.items():
            # Apply filters
            if priority and entry.priority != priority.lower():
                continue
            if category and entry.category != category.lower():
                continue

            symbols.append(symbol)

        return sorted(symbols)

    def get_all_symbols(self) -> List[str]:
        """Get all symbols in watchlist"""
        return sorted(self._watchlist.keys())

    def get_high_priority_symbols(self) -> List[str]:
        """Get high priority symbols"""
        return self.get_symbols(priority="high")

    def get_symbols_by_category(self, category: str) -> List[str]:
        """Get symbols in specific category"""
        return self.get_symbols(category=category)

    def get_watchlist_info(self) -> Dict[str, Any]:
        """Get comprehensive watchlist information"""
        total_symbols = len(self._watchlist)

        # Count by priority
        priority_counts = {"high": 0, "normal": 0, "low": 0}
        category_counts = {}

        for entry in self._watchlist.values():
            priority_counts[entry.priority] = priority_counts.get(entry.priority, 0) + 1
            category_counts[entry.category] = category_counts.get(entry.category, 0) + 1

        return {
            'total_symbols': total_symbols,
            'priority_breakdown': priority_counts,
            'category_breakdown': category_counts,
            'categories': sorted(category_counts.keys()),
            'last_updated': max([entry.added_date for entry in self._watchlist.values()]) if self._watchlist else None
        }

    def get_symbol_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific symbol"""
        symbol = symbol.upper().strip()

        if symbol not in self._watchlist:
            return None

        entry = self._watchlist[symbol]
        return {
            'symbol': entry.symbol,
            'added_date': entry.added_date.isoformat(),
            'priority': entry.priority,
            'category': entry.category,
            'notes': entry.notes,
            'days_on_watchlist': (datetime.now() - entry.added_date).days
        }

    def bulk_add_symbols(self, symbols: List[str], priority: str = "normal",
                        category: str = "general") -> Dict[str, bool]:
        """
        Add multiple symbols at once

        Args:
            symbols: List of symbols to add
            priority: Priority for all symbols
            category: Category for all symbols

        Returns:
            Dictionary mapping symbol to success status
        """
        results = {}

        for symbol in symbols:
            results[symbol] = self.add_symbol(symbol, priority, category)

        return results

    def bulk_remove_symbols(self, symbols: List[str]) -> Dict[str, bool]:
        """
        Remove multiple symbols at once

        Args:
            symbols: List of symbols to remove

        Returns:
            Dictionary mapping symbol to success status
        """
        results = {}

        for symbol in symbols:
            results[symbol] = self.remove_symbol(symbol)

        return results

    def clear_category(self, category: str) -> int:
        """
        Remove all symbols from a specific category

        Args:
            category: Category to clear

        Returns:
            Number of symbols removed
        """
        symbols_to_remove = self.get_symbols_by_category(category)

        for symbol in symbols_to_remove:
            self.remove_symbol(symbol)

        self.logger.info(f"Cleared {len(symbols_to_remove)} symbols from category '{category}'")
        return len(symbols_to_remove)

    def export_watchlist(self, file_path: Optional[str] = None) -> str:
        """
        Export watchlist to JSON file

        Args:
            file_path: Optional custom file path

        Returns:
            Path to exported file
        """
        if not file_path:
            file_path = f"watchlist_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        export_data = {
            'export_date': datetime.now().isoformat(),
            'total_symbols': len(self._watchlist),
            'watchlist': [entry.to_dict() for entry in self._watchlist.values()]
        }

        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2)

        self.logger.info(f"Exported watchlist to {file_path}")
        return file_path

    def import_watchlist(self, file_path: str, merge: bool = True) -> int:
        """
        Import watchlist from JSON file

        Args:
            file_path: Path to JSON file
            merge: If True, merge with existing watchlist. If False, replace.

        Returns:
            Number of symbols imported
        """
        try:
            with open(file_path, 'r') as f:
                import_data = json.load(f)

            if not merge:
                self._watchlist.clear()

            imported_count = 0
            watchlist_data = import_data.get('watchlist', [])

            for entry_data in watchlist_data:
                entry = WatchlistEntry.from_dict(entry_data)
                if entry.symbol not in self._watchlist or not merge:
                    self._watchlist[entry.symbol] = entry
                    imported_count += 1

            self._save_watchlist()

            self.logger.info(f"Imported {imported_count} symbols from {file_path}")
            return imported_count

        except Exception as e:
            self.logger.error(f"Error importing watchlist: {e}")
            return 0

    def _validate_symbol(self, symbol: str) -> bool:
        """Basic symbol validation"""
        if not symbol or len(symbol) < 1 or len(symbol) > 10:
            return False

        # Basic format check (letters, numbers, some special chars)
        if not symbol.replace('.', '').replace('-', '').isalnum():
            return False

        return True

    def _load_watchlist(self):
        """Load watchlist from JSON file"""
        if not os.path.exists(self.watchlist_file):
            self.logger.info("No existing watchlist found, starting fresh")
            return

        try:
            with open(self.watchlist_file, 'r') as f:
                data = json.load(f)

            self._watchlist = {}
            for symbol, entry_data in data.items():
                self._watchlist[symbol] = WatchlistEntry.from_dict(entry_data)

            self.logger.info(f"Loaded {len(self._watchlist)} symbols from watchlist")

        except Exception as e:
            self.logger.error(f"Error loading watchlist: {e}")
            self._watchlist = {}

    def _save_watchlist(self):
        """Save watchlist to JSON file"""
        try:
            # Convert to serializable format
            data = {symbol: entry.to_dict() for symbol, entry in self._watchlist.items()}

            with open(self.watchlist_file, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            self.logger.error(f"Error saving watchlist: {e}")

    # Convenience methods for common operations

    def add_tech_stock(self, symbol: str, notes: str = "") -> bool:
        """Add a tech stock with high priority"""
        return self.add_symbol(symbol, priority="high", category="tech", notes=notes)

    def add_dividend_stock(self, symbol: str, notes: str = "") -> bool:
        """Add a dividend stock with normal priority"""
        return self.add_symbol(symbol, priority="normal", category="dividend", notes=notes)

    def add_growth_stock(self, symbol: str, notes: str = "") -> bool:
        """Add a growth stock with high priority"""
        return self.add_symbol(symbol, priority="high", category="growth", notes=notes)

    def is_watching(self, symbol: str) -> bool:
        """Check if symbol is in watchlist"""
        return symbol.upper().strip() in self._watchlist

    def get_watchlist_for_analysis(self) -> List[str]:
        """Get symbols suitable for strategy analysis (high + normal priority)"""
        return self.get_symbols(priority="high") + self.get_symbols(priority="normal")


# Convenience function for quick watchlist creation
def create_default_watchlist() -> WatchlistManager:
    """Create a watchlist with some default symbols"""
    wm = WatchlistManager()

    # Add some popular symbols if watchlist is empty
    if len(wm.get_all_symbols()) == 0:
        default_symbols = {
            'AAPL': ('tech', 'high', 'Apple Inc.'),
            'GOOGL': ('tech', 'high', 'Alphabet Inc.'),
            'MSFT': ('tech', 'high', 'Microsoft Corp.'),
            'AMZN': ('tech', 'normal', 'Amazon.com Inc.'),
            'TSLA': ('tech', 'normal', 'Tesla Inc.'),
            'NVDA': ('tech', 'high', 'NVIDIA Corp.'),
            'META': ('tech', 'normal', 'Meta Platforms Inc.'),
            'NFLX': ('tech', 'low', 'Netflix Inc.')
        }

        for symbol, (category, priority, notes) in default_symbols.items():
            wm.add_symbol(symbol, priority=priority, category=category, notes=notes)

        logging.info("Created default watchlist with popular tech stocks")

    return wm


if __name__ == "__main__":
    # Example usage
    wm = WatchlistManager()

    # Add some symbols
    wm.add_tech_stock("AAPL", "iPhone maker")
    wm.add_growth_stock("NVDA", "AI chip leader")
    wm.add_dividend_stock("JNJ", "Healthcare dividend aristocrat")

    # Show watchlist info
    print("Watchlist Info:", wm.get_watchlist_info())
    print("All symbols:", wm.get_all_symbols())
    print("High priority:", wm.get_high_priority_symbols())
    print("Tech stocks:", wm.get_symbols_by_category("tech"))

    # Export
    export_path = wm.export_watchlist()
    print(f"Exported to: {export_path}")
