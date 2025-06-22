"""
trading_engine/portfolio_manager.py/tools/position_sizer.py

Pure Position Sizer - Basic Size Up/Down Utilities

Ultra-pure position sizing tool that just does math:
- size_up(symbol, target_pct) -> how much to buy
- size_down(symbol, target_pct) -> how much to sell
- close_position(symbol) -> how much to sell to close
- get_current_size(symbol) -> current position %

NO business logic, NO market decisions, just pure calculations.
Risk manager calls this tool to adjust positions.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
import sys
import os

# Fixed imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
    from Friren_V1.trading_engine.portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface
except ImportError:
    # Fallback stubs
    class TradingDBManager:
        def get_holdings(self, **kwargs): return []
    class SimpleAlpacaInterface:
        def get_account_info(self): return None


@dataclass
class SizeCalculation:
    """Result of pure size calculation"""
    symbol: str
    current_size_pct: float     # Current position as % of portfolio
    target_size_pct: float      # Target position as % of portfolio
    size_change_pct: float      # Change needed (+ = buy more, - = sell some)

    # Dollar amounts
    portfolio_value: float
    current_dollar_amount: float
    target_dollar_amount: float
    trade_dollar_amount: float  # $ amount to trade (+ = buy, - = sell)

    # Share amounts
    current_shares: float
    shares_to_trade: float      # Shares to buy/sell
    current_price: float

    # Simple flags
    is_buy: bool                # True = buy, False = sell
    is_new_position: bool       # True = opening new position
    is_close: bool              # True = closing entire position
    needs_trade: bool           # True = trade amount > minimum threshold

    timestamp: datetime

    def to_trade_request(self) -> Dict[str, Any]:
        """Convert to trade request format for execution engine"""
        return {
            'symbol': self.symbol,
            'action': 'BUY' if self.is_buy else 'SELL',
            'amount': abs(self.trade_dollar_amount) if self.is_buy else None,
            'quantity': self.shares_to_trade if not self.is_buy else None
        }


class PurePositionSizer:
    """
    Pure Position Sizer - Just Math, No Business Logic

    This tool only does position size calculations.
    The risk manager tells it what to do, it calculates how.

    No market regime logic, no sentiment, no decision making.
    Just pure "what does it take to get from A to B" calculations.
    """

    def __init__(self, db_manager: Optional[TradingDBManager] = None,
                 alpaca_interface: Optional[SimpleAlpacaInterface] = None):
        self.logger = logging.getLogger("position_sizer")

        # Tool integrations (for getting current state only)
        self.db_manager = db_manager or TradingDBManager("position_sizer")
        self.alpaca_interface = alpaca_interface

        # Pure configuration (just math thresholds)
        self.config = {
            'min_trade_amount': 100.0,      # $100 minimum trade
            'rebalance_threshold': 0.005,   # 0.5% threshold for rebalancing
            'max_position_pct': 0.20,       # 20% hard limit (safety)
            'price_fallback': 100.0         # Fallback price if unavailable
        }

        self.logger.info("PurePositionSizer initialized")

    def size_up(self, symbol: str, target_pct: float,
                current_price: Optional[float] = None) -> SizeCalculation:
        """
        Calculate what it takes to size UP a position to target percentage

        Args:
            symbol: Stock symbol
            target_pct: Target position size (0.0 to 1.0, e.g., 0.05 = 5%)
            current_price: Current stock price (optional)

        Returns:
            SizeCalculation with buy details
        """
        return self._calculate_size_change(symbol, target_pct, current_price)

    def size_down(self, symbol: str, target_pct: float,
                  current_price: Optional[float] = None) -> SizeCalculation:
        """
        Calculate what it takes to size DOWN a position to target percentage

        Args:
            symbol: Stock symbol
            target_pct: Target position size (0.0 to 1.0)
            current_price: Current stock price (optional)

        Returns:
            SizeCalculation with sell details
        """
        return self._calculate_size_change(symbol, target_pct, current_price)

    def close_position(self, symbol: str,
                      current_price: Optional[float] = None) -> SizeCalculation:
        """
        Calculate what it takes to close entire position (target = 0%)

        Args:
            symbol: Stock symbol
            current_price: Current stock price (optional)

        Returns:
            SizeCalculation with close details
        """
        return self._calculate_size_change(symbol, 0.0, current_price)

    def get_current_size(self, symbol: str) -> float:
        """
        Get current position size as percentage of portfolio

        Args:
            symbol: Stock symbol

        Returns:
            float: Current position size (0.0 to 1.0)
        """
        try:
            current_position = self._get_current_position(symbol)
            return current_position['size_pct']
        except Exception as e:
            self.logger.error(f"Error getting current size for {symbol}: {e}")
            return 0.0

    def get_position_value(self, symbol: str) -> float:
        """
        Get current position value in dollars

        Args:
            symbol: Stock symbol

        Returns:
            float: Position value in dollars
        """
        try:
            current_position = self._get_current_position(symbol)
            return current_position['dollar_amount']
        except Exception as e:
            self.logger.error(f"Error getting position value for {symbol}: {e}")
            return 0.0

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get basic portfolio summary (no business logic)"""
        try:
            portfolio_value = self._get_portfolio_value()
            holdings = self.db_manager.get_holdings(active_only=True)

            positions = []
            total_invested = 0.0

            for holding in holdings:
                invested = float(holding['total_invested'])
                total_invested += invested

                positions.append({
                    'symbol': holding['symbol'],
                    'shares': float(holding['net_quantity']),
                    'invested': invested,
                    'size_pct': (invested / portfolio_value) if portfolio_value > 0 else 0
                })

            return {
                'portfolio_value': portfolio_value,
                'total_invested': total_invested,
                'cash': portfolio_value - total_invested,
                'allocation_pct': (total_invested / portfolio_value) * 100 if portfolio_value > 0 else 0,
                'position_count': len(positions),
                'positions': positions,
                'timestamp': datetime.now()
            }

        except Exception as e:
            self.logger.error(f"Error getting portfolio summary: {e}")
            return {'error': str(e)}

    def can_afford_position(self, symbol: str, target_pct: float) -> Tuple[bool, str]:
        """
        Check if we can afford a target position size

        Args:
            symbol: Stock symbol
            target_pct: Target position percentage

        Returns:
            Tuple[bool, str]: (can_afford, reason)
        """
        try:
            if target_pct > self.config['max_position_pct']:
                return False, f"Exceeds max position limit ({self.config['max_position_pct']:.1%})"

            portfolio_value = self._get_portfolio_value()
            target_dollar_amount = target_pct * portfolio_value

            current_position = self._get_current_position(symbol)
            current_dollar_amount = current_position['dollar_amount']

            additional_investment_needed = target_dollar_amount - current_dollar_amount

            if additional_investment_needed <= 0:
                return True, "Reducing position or no change needed"

            # Get available cash (simplified)
            cash_available = portfolio_value * 0.2  # Assume 20% cash available

            if additional_investment_needed > cash_available:
                return False, f"Need ${additional_investment_needed:,.0f}, have ${cash_available:,.0f} available"

            return True, "Affordable"

        except Exception as e:
            self.logger.error(f"Error checking affordability: {e}")
            return False, f"Error: {e}"

    # Private helper methods (pure calculations only)

    def _calculate_size_change(self, symbol: str, target_pct: float,
                              current_price: Optional[float]) -> SizeCalculation:
        """Core calculation method - pure math only"""
        try:
            # Get current state
            portfolio_value = self._get_portfolio_value()
            current_position = self._get_current_position(symbol)
            price = current_price or self.config['price_fallback']

            # Current position details
            current_size_pct = current_position['size_pct']
            current_dollar_amount = current_position['dollar_amount']
            current_shares = current_position['shares']

            # Target position details
            target_dollar_amount = target_pct * portfolio_value

            # Calculate changes needed
            size_change_pct = target_pct - current_size_pct
            trade_dollar_amount = size_change_pct * portfolio_value
            shares_to_trade = abs(trade_dollar_amount) / price if price > 0 else 0

            # Determine trade characteristics
            is_buy = trade_dollar_amount > 0
            is_new_position = current_shares == 0 and target_pct > 0
            is_close = target_pct == 0 and current_shares != 0
            needs_trade = (abs(size_change_pct) > self.config['rebalance_threshold'] and
                          abs(trade_dollar_amount) > self.config['min_trade_amount'])

            return SizeCalculation(
                symbol=symbol,
                current_size_pct=current_size_pct,
                target_size_pct=target_pct,
                size_change_pct=size_change_pct,
                portfolio_value=portfolio_value,
                current_dollar_amount=current_dollar_amount,
                target_dollar_amount=target_dollar_amount,
                trade_dollar_amount=trade_dollar_amount,
                current_shares=current_shares,
                shares_to_trade=shares_to_trade,
                current_price=price,
                is_buy=is_buy,
                is_new_position=is_new_position,
                is_close=is_close,
                needs_trade=needs_trade,
                timestamp=datetime.now()
            )

        except Exception as e:
            self.logger.error(f"Error calculating size change for {symbol}: {e}")
            # Return safe default
            return self._create_safe_default(symbol, target_pct)

    def _get_portfolio_value(self) -> float:
        """Get total portfolio value (no business logic)"""
        try:
            if self.alpaca_interface:
                account = self.alpaca_interface.get_account_info()
                if account:
                    return account.portfolio_value

            # Fallback: estimate from database
            holdings = self.db_manager.get_holdings(active_only=True)
            total_invested = sum(float(h['total_invested']) for h in holdings)
            estimated_cash = 25000.0  # Conservative estimate

            return total_invested + estimated_cash

        except Exception as e:
            self.logger.error(f"Error getting portfolio value: {e}")
            return 50000.0  # Safe fallback

    def _get_current_position(self, symbol: str) -> Dict[str, float]:
        """Get current position details (no business logic)"""
        try:
            holdings = self.db_manager.get_holdings(symbol=symbol, active_only=True)

            if holdings:
                holding = holdings[0]
                shares = float(holding['net_quantity'])
                total_invested = float(holding['total_invested'])
                cost_basis = float(holding['avg_cost_basis'])

                portfolio_value = self._get_portfolio_value()
                size_pct = (total_invested / portfolio_value) if portfolio_value > 0 else 0

                return {
                    'shares': shares,
                    'dollar_amount': total_invested,
                    'size_pct': size_pct,
                    'cost_basis': cost_basis
                }
            else:
                # No position
                return {
                    'shares': 0.0,
                    'dollar_amount': 0.0,
                    'size_pct': 0.0,
                    'cost_basis': 0.0
                }

        except Exception as e:
            self.logger.error(f"Error getting position for {symbol}: {e}")
            return {'shares': 0.0, 'dollar_amount': 0.0, 'size_pct': 0.0, 'cost_basis': 0.0}

    def _create_safe_default(self, symbol: str, target_pct: float) -> SizeCalculation:
        """Create safe default calculation when errors occur"""
        return SizeCalculation(
            symbol=symbol,
            current_size_pct=0.0,
            target_size_pct=target_pct,
            size_change_pct=0.0,
            portfolio_value=50000.0,
            current_dollar_amount=0.0,
            target_dollar_amount=0.0,
            trade_dollar_amount=0.0,
            current_shares=0.0,
            shares_to_trade=0.0,
            current_price=100.0,
            is_buy=False,
            is_new_position=False,
            is_close=False,
            needs_trade=False,
            timestamp=datetime.now()
        )


# Pure utility functions for external use

def calculate_position_change(symbol: str, from_pct: float, to_pct: float,
                            portfolio_value: float, current_price: float) -> Dict[str, float]:
    """
    Pure math function to calculate position changes

    Args:
        symbol: Stock symbol
        from_pct: Current position size (0.0 to 1.0)
        to_pct: Target position size (0.0 to 1.0)
        portfolio_value: Total portfolio value
        current_price: Current stock price

    Returns:
        Dict with calculation results
    """
    size_change_pct = to_pct - from_pct
    trade_dollar_amount = size_change_pct * portfolio_value
    shares_to_trade = abs(trade_dollar_amount) / current_price if current_price > 0 else 0

    return {
        'size_change_pct': size_change_pct,
        'trade_dollar_amount': trade_dollar_amount,
        'shares_to_trade': shares_to_trade,
        'is_buy': trade_dollar_amount > 0,
        'needs_trade': abs(trade_dollar_amount) > 100  # $100 minimum
    }

def dollars_to_shares(dollar_amount: float, price: float) -> float:
    """Convert dollar amount to number of shares"""
    return dollar_amount / price if price > 0 else 0

def shares_to_dollars(shares: float, price: float) -> float:
    """Convert number of shares to dollar amount"""
    return shares * price

def calculate_position_pct(dollar_amount: float, portfolio_value: float) -> float:
    """Calculate position size as percentage of portfolio"""
    return (dollar_amount / portfolio_value) if portfolio_value > 0 else 0
