"""
trading_engine/portfolio_manager.py/tools/account_manager.py

Account Management with Hybrid Storage Strategy

This tool implements the optimal account data strategy:
- Alpaca API: Real-time account values, cash, buying power
- Local Database: Total profits, performance analytics, trade history
- Smart Caching: Reduces API calls while maintaining accuracy

Key Features:
- Account snapshot caching (30-second refresh)
- Performance calculation from trade history
- Risk metrics calculation
- Portfolio value tracking over time
"""

import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
import logging
import json
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from .db_manager import TradingDBManager
    from .alpaca_interface import SimpleAlpacaInterface, AlpacaAccount
except ImportError:
    # Fallback stubs for testing
    class TradingDBManager:
        def __init__(self, process_name: str = "account_manager"):
            self.logger = logging.getLogger(f"db.{process_name}")
        def get_transactions(self, **kwargs): return []
        def get_holdings(self, **kwargs): return []
        def get_portfolio_summary(self): return {}

    class SimpleAlpacaInterface:
        def get_account_info(self): return None

    @dataclass
    class AlpacaAccount:
        account_id: str = "STUB"
        buying_power: float = 25000.0
        cash: float = 25000.0
        portfolio_value: float = 50000.0
        equity: float = 50000.0
        day_trade_buying_power: float = 100000.0
        initial_margin: float = 0.0
        maintenance_margin: float = 0.0
        last_equity: float = 50000.0


@dataclass
class AccountSnapshot:
    """Local account snapshot for performance and caching"""
    timestamp: datetime

    # From Alpaca (real-time)
    portfolio_value: float
    cash: float
    buying_power: float
    equity: float
    day_trade_buying_power: float
    last_equity: float

    # Calculated locally (performance tracking)
    total_realized_pnl: float
    total_unrealized_pnl: float
    total_pnl: float

    # Performance metrics
    day_pnl: float
    day_pnl_pct: float
    week_pnl: float
    month_pnl: float

    # Portfolio allocation
    total_invested: float
    cash_percentage: float
    allocation_percentage: float

    # Risk metrics
    max_drawdown: float
    volatility: float
    sharpe_ratio: float

    # Trading activity
    trades_today: int
    trades_this_week: int
    trades_this_month: int

    # Cache metadata
    last_alpaca_sync: datetime
    last_db_sync: datetime
    cache_valid: bool = True


@dataclass
class PerformanceMetrics:
    """Detailed performance analytics"""
    total_return: float
    total_return_pct: float
    annualized_return: float
    max_drawdown: float
    max_drawdown_duration: int  # days
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    win_rate: float
    average_win: float
    average_loss: float
    profit_factor: float
    largest_win: float
    largest_loss: float
    consecutive_wins: int
    consecutive_losses: int
    total_trades: int
    winning_trades: int
    losing_trades: int


class AccountManager:
    """
    Smart Account Management with Hybrid Storage

    Strategy:
    1. Cache Alpaca account data (30-second refresh)
    2. Calculate performance metrics from trade history
    3. Provide unified account view for trading system
    4. Minimize API calls while maintaining accuracy
    """

    def __init__(self,
                 db_manager: Optional[TradingDBManager] = None,
                 alpaca_interface: Optional[SimpleAlpacaInterface] = None,
                 cache_duration: int = 30):
        """
        Initialize Account Manager

        Args:
            db_manager: Database manager for trade history
            alpaca_interface: Alpaca API interface
            cache_duration: Cache duration in seconds (default 30)
        """
        self.logger = logging.getLogger("account_manager")

        # Dependencies
        self.db_manager = db_manager or TradingDBManager("account_manager")
        self.alpaca_interface = alpaca_interface or SimpleAlpacaInterface()

        # Cache configuration
        self.cache_duration = timedelta(seconds=cache_duration)
        self._cached_snapshot: Optional[AccountSnapshot] = None
        self._last_cache_time: Optional[datetime] = None

        # Performance tracking
        self._daily_snapshots: List[AccountSnapshot] = []
        self._performance_cache: Optional[PerformanceMetrics] = None
        self._performance_cache_time: Optional[datetime] = None

        self.logger.info("AccountManager initialized")

    def get_account_snapshot(self, force_refresh: bool = False) -> AccountSnapshot:
        """
        Get current account snapshot (cached or fresh)

        Args:
            force_refresh: Force refresh from APIs even if cache is valid

        Returns:
            AccountSnapshot: Current account state
        """
        now = datetime.now()

        # Check if cache is still valid
        if (not force_refresh and
            self._cached_snapshot and
            self._last_cache_time and
            (now - self._last_cache_time) < self.cache_duration):

            self.logger.debug("Returning cached account snapshot")
            return self._cached_snapshot

        # Refresh snapshot
        self.logger.info("Refreshing account snapshot from APIs")
        snapshot = self._create_fresh_snapshot()

        # Update cache
        self._cached_snapshot = snapshot
        self._last_cache_time = now

        # Store daily snapshot for performance tracking
        self._store_daily_snapshot(snapshot)

        return snapshot

    def get_performance_metrics(self, force_refresh: bool = False) -> PerformanceMetrics:
        """
        Get detailed performance metrics

        Args:
            force_refresh: Force recalculation of metrics

        Returns:
            PerformanceMetrics: Detailed performance analytics
        """
        now = datetime.now()

        # Check performance cache (refresh every 5 minutes)
        if (not force_refresh and
            self._performance_cache and
            self._performance_cache_time and
            (now - self._performance_cache_time) < timedelta(minutes=5)):

            return self._performance_cache

        # Calculate fresh performance metrics
        self.logger.info("Calculating performance metrics")
        metrics = self._calculate_performance_metrics()

        # Update cache
        self._performance_cache = metrics
        self._performance_cache_time = now

        return metrics

    def get_cash_available(self) -> float:
        """Get available cash (from cache or fresh)"""
        snapshot = self.get_account_snapshot()
        return snapshot.cash

    def get_buying_power(self) -> float:
        """Get buying power (from cache or fresh)"""
        snapshot = self.get_account_snapshot()
        return snapshot.buying_power

    def get_portfolio_value(self) -> float:
        """Get total portfolio value (from cache or fresh)"""
        snapshot = self.get_account_snapshot()
        return snapshot.portfolio_value

    def get_total_pnl(self) -> float:
        """Get total profit/loss (calculated locally)"""
        snapshot = self.get_account_snapshot()
        return snapshot.total_pnl

    def get_day_pnl(self) -> Tuple[float, float]:
        """Get today's P&L in dollars and percentage"""
        snapshot = self.get_account_snapshot()
        return snapshot.day_pnl, snapshot.day_pnl_pct

    def is_account_healthy(self) -> Tuple[bool, str]:
        """
        Check if account is in good health

        Returns:
            Tuple[bool, str]: (is_healthy, status_message)
        """
        try:
            snapshot = self.get_account_snapshot()

            # Health checks
            issues = []

            # Check buying power
            if snapshot.buying_power < 1000:
                issues.append(f"Low buying power: ${snapshot.buying_power:,.2f}")

            # Check daily loss
            if snapshot.day_pnl_pct < -5.0:
                issues.append(f"High daily loss: {snapshot.day_pnl_pct:.1f}%")

            # Check allocation
            if snapshot.allocation_percentage > 95:
                issues.append(f"Over-allocated: {snapshot.allocation_percentage:.1f}%")

            # Check cache freshness
            cache_age = (datetime.now() - snapshot.timestamp).total_seconds()
            if cache_age > 300:  # 5 minutes
                issues.append(f"Stale data: {cache_age:.0f}s old")

            if issues:
                return False, "; ".join(issues)
            else:
                return True, "Account healthy"

        except Exception as e:
            self.logger.error(f"Error checking account health: {e}")
            return False, f"Health check failed: {e}"

    def _create_fresh_snapshot(self) -> AccountSnapshot:
        """Create fresh account snapshot from APIs and database"""
        now = datetime.now()

        # Get Alpaca account data
        alpaca_account = self._get_alpaca_data()

        # Get local data from database
        db_data = self._get_database_data()

        # Calculate P&L metrics
        pnl_metrics = self._calculate_pnl_metrics(alpaca_account, db_data)

        # Calculate performance metrics
        performance = self._calculate_daily_performance(alpaca_account)

        # Calculate portfolio allocation
        allocation = self._calculate_allocation(alpaca_account, db_data)

        # Get trading activity
        activity = self._get_trading_activity()

        return AccountSnapshot(
            timestamp=now,

            # Alpaca data
            portfolio_value=alpaca_account.portfolio_value,
            cash=alpaca_account.cash,
            buying_power=alpaca_account.buying_power,
            equity=alpaca_account.equity,
            day_trade_buying_power=alpaca_account.day_trade_buying_power,
            last_equity=alpaca_account.last_equity,

            # P&L metrics
            total_realized_pnl=pnl_metrics['realized'],
            total_unrealized_pnl=pnl_metrics['unrealized'],
            total_pnl=pnl_metrics['total'],

            # Performance
            day_pnl=performance['day_pnl'],
            day_pnl_pct=performance['day_pnl_pct'],
            week_pnl=performance['week_pnl'],
            month_pnl=performance['month_pnl'],

            # Allocation
            total_invested=allocation['invested'],
            cash_percentage=allocation['cash_pct'],
            allocation_percentage=allocation['allocation_pct'],

            # Risk metrics (simplified for now)
            max_drawdown=0.0,  # TODO: Calculate from historical data
            volatility=0.0,    # TODO: Calculate from returns
            sharpe_ratio=0.0,  # TODO: Calculate from returns

            # Activity
            trades_today=activity['today'],
            trades_this_week=activity['week'],
            trades_this_month=activity['month'],

            # Cache metadata
            last_alpaca_sync=now,
            last_db_sync=now,
            cache_valid=True
        )

    def _get_alpaca_data(self) -> AlpacaAccount:
        """Get account data from Alpaca API"""
        try:
            account = self.alpaca_interface.get_account_info()
            if account:
                return account
            else:
                # Return fallback data
                self.logger.warning("Alpaca API unavailable, using fallback data")
                return AlpacaAccount()

        except Exception as e:
            self.logger.error(f"Error getting Alpaca account data: {e}")
            return AlpacaAccount()

    def _get_database_data(self) -> Dict[str, Any]:
        """Get relevant data from local database"""
        try:
            holdings = self.db_manager.get_holdings(active_only=True)
            portfolio_summary = self.db_manager.get_portfolio_summary()

            return {
                'holdings': holdings,
                'portfolio_summary': portfolio_summary
            }

        except Exception as e:
            self.logger.error(f"Error getting database data: {e}")
            return {'holdings': [], 'portfolio_summary': {}}

    def _calculate_pnl_metrics(self, alpaca_account: AlpacaAccount, db_data: Dict) -> Dict[str, float]:
        """Calculate P&L metrics from available data"""
        try:
            # Get unrealized P&L from current positions
            portfolio_summary = db_data.get('portfolio_summary', {})
            total_invested = float(portfolio_summary.get('total_invested', 0))

            # Unrealized = Current Value - Invested
            unrealized_pnl = alpaca_account.portfolio_value - total_invested - alpaca_account.cash

            # TODO: Calculate realized P&L from trade history
            realized_pnl = 0.0

            total_pnl = realized_pnl + unrealized_pnl

            return {
                'realized': realized_pnl,
                'unrealized': unrealized_pnl,
                'total': total_pnl
            }

        except Exception as e:
            self.logger.error(f"Error calculating P&L metrics: {e}")
            return {'realized': 0.0, 'unrealized': 0.0, 'total': 0.0}

    def _calculate_daily_performance(self, alpaca_account: AlpacaAccount) -> Dict[str, float]:
        """Calculate daily, weekly, monthly performance"""
        try:
            # Use last_equity for daily performance
            day_pnl = alpaca_account.equity - alpaca_account.last_equity
            day_pnl_pct = (day_pnl / alpaca_account.last_equity) * 100 if alpaca_account.last_equity > 0 else 0

            # TODO: Implement week/month calculations from historical snapshots
            week_pnl = 0.0
            month_pnl = 0.0

            return {
                'day_pnl': day_pnl,
                'day_pnl_pct': day_pnl_pct,
                'week_pnl': week_pnl,
                'month_pnl': month_pnl
            }

        except Exception as e:
            self.logger.error(f"Error calculating daily performance: {e}")
            return {'day_pnl': 0.0, 'day_pnl_pct': 0.0, 'week_pnl': 0.0, 'month_pnl': 0.0}

    def _calculate_allocation(self, alpaca_account: AlpacaAccount, db_data: Dict) -> Dict[str, float]:
        """Calculate portfolio allocation metrics"""
        try:
            portfolio_summary = db_data.get('portfolio_summary', {})
            total_invested = float(portfolio_summary.get('total_invested', 0))

            cash_pct = (alpaca_account.cash / alpaca_account.portfolio_value) * 100 if alpaca_account.portfolio_value > 0 else 0
            allocation_pct = (total_invested / alpaca_account.portfolio_value) * 100 if alpaca_account.portfolio_value > 0 else 0

            return {
                'invested': total_invested,
                'cash_pct': cash_pct,
                'allocation_pct': allocation_pct
            }

        except Exception as e:
            self.logger.error(f"Error calculating allocation: {e}")
            return {'invested': 0.0, 'cash_pct': 100.0, 'allocation_pct': 0.0}

    def _get_trading_activity(self) -> Dict[str, int]:
        """Get trading activity counts"""
        try:
            # TODO: Implement proper date filtering in database queries
            # For now, return mock data
            return {
                'today': 0,
                'week': 0,
                'month': 0
            }

        except Exception as e:
            self.logger.error(f"Error getting trading activity: {e}")
            return {'today': 0, 'week': 0, 'month': 0}

    def _calculate_performance_metrics(self) -> PerformanceMetrics:
        """Calculate detailed performance metrics"""
        # TODO: Implement comprehensive performance calculations
        # This would analyze trade history for win/loss ratios, Sharpe ratio, etc.

        return PerformanceMetrics(
            total_return=0.0,
            total_return_pct=0.0,
            annualized_return=0.0,
            max_drawdown=0.0,
            max_drawdown_duration=0,
            volatility=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=0.0,
            calmar_ratio=0.0,
            win_rate=0.0,
            average_win=0.0,
            average_loss=0.0,
            profit_factor=0.0,
            largest_win=0.0,
            largest_loss=0.0,
            consecutive_wins=0,
            consecutive_losses=0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0
        )

    def _store_daily_snapshot(self, snapshot: AccountSnapshot):
        """Store daily snapshot for historical analysis"""
        # Keep only last 30 days
        cutoff_date = datetime.now() - timedelta(days=30)
        self._daily_snapshots = [s for s in self._daily_snapshots if s.timestamp > cutoff_date]

        # Add today's snapshot
        self._daily_snapshots.append(snapshot)

    def health_check(self) -> Dict[str, Any]:
        """Account manager health check"""
        try:
            is_healthy, message = self.is_account_healthy()

            return {
                "component": "account_manager",
                "status": "healthy" if is_healthy else "warning",
                "message": message,
                "cache_age_seconds": (datetime.now() - (self._last_cache_time or datetime.now())).total_seconds(),
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            return {
                "component": "account_manager",
                "status": "error",
                "message": f"Health check failed: {e}",
                "timestamp": datetime.now().isoformat()
            }


# ========== USAGE EXAMPLES ==========

if __name__ == "__main__":
    # Example usage

    # Initialize account manager
    account_manager = AccountManager()

    # Get current account snapshot
    snapshot = account_manager.get_account_snapshot()
    print(f"Portfolio Value: ${snapshot.portfolio_value:,.2f}")
    print(f"Cash Available: ${snapshot.cash:,.2f}")
    print(f"Buying Power: ${snapshot.buying_power:,.2f}")
    print(f"Total P&L: ${snapshot.total_pnl:,.2f}")
    print(f"Day P&L: ${snapshot.day_pnl:,.2f} ({snapshot.day_pnl_pct:.1f}%)")

    # Check account health
    is_healthy, message = account_manager.is_account_healthy()
    print(f"Account Health: {'HEALTHY' if is_healthy else 'WARNING'} - {message}")

    # Get performance metrics
    performance = account_manager.get_performance_metrics()
    print(f"Total Return: {performance.total_return_pct:.1f}%")
    print(f"Win Rate: {performance.win_rate:.1f}%")
