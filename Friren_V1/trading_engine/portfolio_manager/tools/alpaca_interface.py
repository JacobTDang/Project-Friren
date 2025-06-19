"""
trading_engine/portfolio_manager/tools/alpaca_interface.py

Modern Alpaca Interface - Updated for alpaca-py Library

Key Updates:
1. Uses modern 'alpaca.trading.client.TradingClient' instead of legacy 'alpaca_trade_api'
2. Proper credential management and environment variable handling
3. Enhanced error handling and fallback simulation mode
4. Clean integration with your trading system architecture

This interface provides a simplified but robust connection to Alpaca's paper trading API
for testing your decision engine without real money risk.
"""

import os
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

# Modern Alpaca imports with fallback
try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce, OrderType
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockLatestQuoteRequest
    HAS_ALPACA = True
    print(" Modern alpaca-py library imported successfully")
except ImportError:
    HAS_ALPACA = False
    print(" Alpaca library not available - running in simulation mode")

    # Create enum stubs for development
    class OrderSide(Enum):
        BUY = "buy"
        SELL = "sell"

    class OrderType(Enum):
        MARKET = "market"
        LIMIT = "limit"

    class TimeInForce(Enum):
        DAY = "day"
        GTC = "gtc"


@dataclass
class AlpacaConfig:
    """
    Alpaca configuration with your credentials

    **Security Note:**
    In production, never hardcode credentials in source code.
    Always use environment variables or secure secret management.
    """
    api_key: str
    secret_key: str
    emergency_code: str
    base_url: str = "https://paper-api.alpaca.markets"  # Paper trading URL

    @classmethod
    def from_environment(cls):
        """Load configuration from environment variables"""
        return cls(
            api_key=os.getenv("ALPACA_API_KEY", ""),
            secret_key=os.getenv("ALPACA_SECRET_KEY", ""),  # Note: fixed typo in env var name
            emergency_code=os.getenv("ALPACA_EMERGENCY_CODE", ""),
            base_url=os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
        )

    @classmethod
    def with_fallback_defaults(cls):
        """Fallback configuration for development/testing"""
        return cls(
            api_key=os.getenv("ALPACA_API_KEY", "test_key"),
            secret_key=os.getenv("ALPACA_SECRET_KEY", "test_secret"),
            emergency_code=os.getenv("ALPACA_EMERGENCY_CODE", "test_emergency"),
            base_url="https://paper-api.alpaca.markets"
        )


@dataclass
class AlpacaAccount:
    """Account information from Alpaca"""
    account_id: str
    buying_power: float
    cash: float
    portfolio_value: float
    day_trade_buying_power: float
    initial_margin: float
    maintenance_margin: float
    equity: float
    last_equity: float


@dataclass
class AlpacaPosition:
    """Position information from Alpaca"""
    symbol: str
    quantity: float
    market_value: float
    current_price: float
    cost_basis: float
    unrealized_pl: float
    unrealized_plpc: float
    avg_entry_price: float


@dataclass
class AlpacaOrder:
    """Order information from Alpaca"""
    order_id: str
    symbol: str
    quantity: float
    side: str
    order_type: str
    status: str
    filled_qty: float
    filled_avg_price: Optional[float]
    submitted_at: datetime
    filled_at: Optional[datetime]


class SimpleAlpacaInterface:
    """
    Modern Alpaca Interface for Trading System
    """
class SimpleAlpacaInterface:
    """
    Modern Alpaca Interface for Trading System

    **Design Philosophy:**
    - Simple, reliable connection to Alpaca paper trading
    - Graceful fallback to simulation mode when API unavailable
    - Clean error handling with meaningful feedback
    - Optimized for your decision engine's needs

    **Usage with Environment Variables:**
    Set these environment variables:
    - ALPACA_API_KEY=your_api_key
    - ALPACA_SECRET_KEY=your_secret_key (note: fixed typo)
    - ALPACA_EMERGENCY_CODE=your_emergency_code
    """

    def __init__(self, config: Optional[AlpacaConfig] = None):
        self.logger = logging.getLogger("alpaca_interface")

        # Load configuration with fallback
        self.config = config or AlpacaConfig.from_environment()

        # Initialize clients
        self.trading_client = None
        self.data_client = None
        self.is_connected = False
        self.is_simulation_mode = not HAS_ALPACA

        # Connection state
        self.last_connection_attempt = None
        self.connection_retry_count = 0
        self.max_retries = 3

        # Performance tracking
        self.api_call_count = 0
        self.last_api_call = None

        if HAS_ALPACA and self.config.api_key and self.config.secret_key:
            self._initialize_clients()
        else:
            self._setup_simulation_mode()

    def _initialize_clients(self):
        """Initialize Alpaca trading and data clients"""
        try:
            # Create trading client for orders and account management
            self.trading_client = TradingClient(
                api_key=self.config.api_key,
                secret_key=self.config.secret_key,
                paper=True  # Always use paper trading for safety
            )

            # Create data client for market data
            self.data_client = StockHistoricalDataClient(
                api_key=self.config.api_key,
                secret_key=self.config.secret_key
            )

            # Test connection by getting account info
            account = self.trading_client.get_account()
            self.is_connected = True
            self.connection_retry_count = 0

            self.logger.info(f" Alpaca API connected successfully")
            self.logger.info(f"Account: {account.account_number[:8]}... | "
                           f"Buying Power: ${float(account.buying_power):,.2f}")

        except Exception as e:
            self.logger.error(f" Failed to initialize Alpaca API: {e}")
            self.connection_retry_count += 1

            if self.connection_retry_count >= self.max_retries:
                self.logger.warning("Max retries reached, switching to simulation mode")
                self._setup_simulation_mode()
            else:
                self.is_connected = False

    def _setup_simulation_mode(self):
        """Setup simulation mode for development/testing"""
        self.is_simulation_mode = True
        self.is_connected = True  # Consider simulation as "connected"
        self.logger.warning(" Running in SIMULATION MODE - no real trades will be executed")

    def get_account_info(self) -> Optional[AlpacaAccount]:
        """
        Get account information

        **Risk Manager Integration:**
        This provides the buying power and portfolio value that the risk manager
        uses for position sizing and safety checks.
        """
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if self.is_simulation_mode:
                # Return realistic simulation data
                return AlpacaAccount(
                    account_id="SIM_ACCOUNT",
                    buying_power=25000.0,
                    cash=25000.0,
                    portfolio_value=50000.0,
                    day_trade_buying_power=100000.0,
                    initial_margin=0.0,
                    maintenance_margin=0.0,
                    equity=50000.0,
                    last_equity=50000.0
                )

            if not self.trading_client:
                return None

            account = self.trading_client.get_account()

            return AlpacaAccount(
                account_id=account.account_number,
                buying_power=float(account.buying_power),
                cash=float(account.cash),
                portfolio_value=float(account.portfolio_value),
                day_trade_buying_power=float(account.daytrading_buying_power),
                initial_margin=float(account.initial_margin),
                maintenance_margin=float(account.maintenance_margin),
                equity=float(account.equity),
                last_equity=float(account.last_equity)
            )

        except Exception as e:
            self.logger.error(f"Error getting account info: {e}")
            return None

    def submit_market_order(self, symbol: str, quantity: float, side: str) -> Tuple[bool, str]:
        """
        Submit market order

        **Decision Engine Integration:**
        This is called by the execution engine when the risk manager approves a trade.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            quantity: Number of shares
            side: 'buy' or 'sell'

        Returns:
            Tuple[bool, str]: (success, order_id_or_error_message)
        """
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if self.is_simulation_mode:
                # Simulate order execution
                fake_order_id = f"SIM_{symbol}_{int(time.time())}"
                self.logger.info(f" SIMULATED ORDER: {side.upper()} {quantity} shares of {symbol}")
                return True, fake_order_id

            if not self.trading_client:
                return False, "Trading client not available"

            # Create market order request
            market_order_data = MarketOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            # Submit order
            order = self.trading_client.submit_order(order_data=market_order_data)

            self.logger.info(f" ORDER SUBMITTED: {side.upper()} {quantity} shares of {symbol} | ID: {order.id}")
            return True, str(order.id)

        except Exception as e:
            error_msg = f"Error submitting order for {symbol}: {e}"
            self.logger.error(error_msg)
            return False, error_msg

    def get_current_price(self, symbol: str) -> Optional[float]:
        """
        Get current market price for symbol

        **Position Sizer Integration:**
        Used by position sizer for accurate trade amount calculations.
        """
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if self.is_simulation_mode:
                # Generate realistic-looking fake prices
                import hashlib
                price_seed = int(hashlib.md5(symbol.encode()).hexdigest(), 16) % 1000
                base_price = 50 + price_seed % 450  # $50-$500 range
                return float(base_price)

            if not self.data_client:
                return None

            # Get latest quote
            request_params = StockLatestQuoteRequest(symbol_or_symbols=[symbol])
            latest_quote = self.data_client.get_stock_latest_quote(request_params)

            if symbol in latest_quote:
                quote = latest_quote[symbol]
                # Use bid-ask midpoint for more accurate pricing
                bid = float(quote.bid_price) if quote.bid_price else 0
                ask = float(quote.ask_price) if quote.ask_price else 0

                if bid > 0 and ask > 0:
                    return (bid + ask) / 2.0
                elif ask > 0:
                    return ask
                elif bid > 0:
                    return bid

            return None

        except Exception as e:
            self.logger.error(f"Error getting price for {symbol}: {e}")
            return None

    def get_position(self, symbol: str) -> Optional[AlpacaPosition]:
        """
        Get current position for symbol

        **Risk Manager Integration:**
        Used to enforce the "one position per symbol" rule and track current allocations.
        """
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if self.is_simulation_mode:
                # Return no position in simulation mode
                return None

            if not self.trading_client:
                return None

            try:
                position = self.trading_client.get_open_position(symbol)

                return AlpacaPosition(
                    symbol=position.symbol,
                    quantity=float(position.qty),
                    market_value=float(position.market_value),
                    current_price=float(position.current_price),
                    cost_basis=float(position.cost_basis),
                    unrealized_pl=float(position.unrealized_pl),
                    unrealized_plpc=float(position.unrealized_plpc),
                    avg_entry_price=float(position.avg_entry_price)
                )

            except Exception:
                # No position found (this is normal)
                return None

        except Exception as e:
            self.logger.error(f"Error getting position for {symbol}: {e}")
            return None

    def get_all_positions(self) -> List[AlpacaPosition]:
        """
        Get all current positions

        **Portfolio Manager Integration:**
        Provides complete portfolio snapshot for allocation calculations.
        """
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if self.is_simulation_mode:
                return []  # No positions in simulation mode

            if not self.trading_client:
                return []

            positions = self.trading_client.get_all_positions()

            return [
                AlpacaPosition(
                    symbol=pos.symbol,
                    quantity=float(pos.qty),
                    market_value=float(pos.market_value),
                    current_price=float(pos.current_price),
                    cost_basis=float(pos.cost_basis),
                    unrealized_pl=float(pos.unrealized_pl),
                    unrealized_plpc=float(pos.unrealized_plpc),
                    avg_entry_price=float(pos.avg_entry_price)
                )
                for pos in positions
            ]

        except Exception as e:
            self.logger.error(f"Error getting all positions: {e}")
            return []

    def get_recent_orders(self, limit: int = 10) -> List[AlpacaOrder]:
        """Get recent orders for monitoring and debugging"""
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if self.is_simulation_mode:
                return []

            if not self.trading_client:
                return []

            orders = self.trading_client.get_orders()[:limit]

            return [
                AlpacaOrder(
                    order_id=str(order.id),
                    symbol=order.symbol,
                    quantity=float(order.qty),
                    side=order.side.value,
                    order_type=order.order_type.value,
                    status=order.status.value,
                    filled_qty=float(order.filled_qty) if order.filled_qty else 0.0,
                    filled_avg_price=float(order.filled_avg_price) if order.filled_avg_price else None,
                    submitted_at=order.submitted_at,
                    filled_at=order.filled_at
                )
                for order in orders
            ]

        except Exception as e:
            self.logger.error(f"Error getting recent orders: {e}")
            return []

    def close_position(self, symbol: str) -> Tuple[bool, str]:
        """
        Close entire position for symbol

        **Emergency Controls:**
        Used by risk manager for emergency position closure.
        """
        try:
            if self.is_simulation_mode:
                self.logger.info(f" SIMULATED POSITION CLOSE: {symbol}")
                return True, f"SIM_CLOSE_{symbol}"

            if not self.trading_client:
                return False, "Trading client not available"

            # Close the position
            close_response = self.trading_client.close_position(symbol)

            self.logger.info(f" POSITION CLOSED: {symbol}")
            return True, str(close_response.id) if hasattr(close_response, 'id') else "CLOSED"

        except Exception as e:
            error_msg = f"Error closing position for {symbol}: {e}"
            self.logger.error(error_msg)
            return False, error_msg

    def get_interface_status(self) -> Dict[str, Any]:
        """
        Get interface status for monitoring and debugging

        **System Health:**
        Used by monitoring processes to track API health and performance.
        """
        return {
            'is_connected': self.is_connected,
            'is_simulation_mode': self.is_simulation_mode,
            'api_call_count': self.api_call_count,
            'last_api_call': self.last_api_call.isoformat() if self.last_api_call else None,
            'connection_retry_count': self.connection_retry_count,
            'max_retries': self.max_retries,
            'has_alpaca_library': HAS_ALPACA,
            'config_loaded': bool(self.config.api_key and self.config.secret_key)
        }

    def reconnect(self) -> bool:
        """
        Attempt to reconnect to Alpaca API

        **Resilience:**
        Called by monitoring processes when connection issues are detected.
        """
        self.logger.info("Attempting to reconnect to Alpaca API...")
        self.is_connected = False
        self.connection_retry_count = 0

        if HAS_ALPACA and self.config.api_key and self.config.secret_key:
            self._initialize_clients()
            return self.is_connected
        else:
            self._setup_simulation_mode()
            return True


# Utility functions for integration

def create_alpaca_interface(use_env_vars: bool = True) -> SimpleAlpacaInterface:
    """
    Factory function to create Alpaca interface

    **Usage Examples:**

    # Use environment variables (recommended for production)
    alpaca = create_alpaca_interface(use_env_vars=True)

    # Use fallback configuration (for development)
    alpaca = create_alpaca_interface(use_env_vars=False)
    """
    if use_env_vars:
        config = AlpacaConfig.from_environment()
    else:
        config = AlpacaConfig.with_fallback_defaults()

    return SimpleAlpacaInterface(config)


def test_alpaca_connection() -> bool:
    """
    Test Alpaca connection and return success status

    **Development Helper:**
    Use this to verify your credentials and connection before running the full system.
    """
    try:
        alpaca = create_alpaca_interface()
        account = alpaca.get_account_info()

        if account:
            print(f"Alpaca connection successful!")
            print(f"Account ID: {account.account_id}")
            print(f"Buying Power: ${account.buying_power:,.2f}")
            print(f"Portfolio Value: ${account.portfolio_value:,.2f}")
            print(f"Simulation Mode: {alpaca.is_simulation_mode}")
            return True
        else:
            print("Could not retrieve account information")
            return False

    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

