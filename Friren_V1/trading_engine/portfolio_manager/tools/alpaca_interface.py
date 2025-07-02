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
    base_url: str = "https://paper-api.alpaca.markets/v2"  # Paper trading URL with v2 endpoint

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
    def __init__(self, config: Optional[AlpacaConfig] = None):
        """
        Initialize Alpaca interface - PRODUCTION ONLY

        Args:
            config: Alpaca configuration. If None, loads from environment variables.
        """
        self.logger = logging.getLogger("alpaca_interface")

        # Load configuration from environment - NO FALLBACKS
        self.config = config or AlpacaConfig.from_environment()

        # Validate configuration
        if not self.config.api_key or not self.config.secret_key:
            raise ValueError("PRODUCTION: Alpaca API credentials required. Set ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables.")

        # Production mode only - no simulation
        self.is_simulation_mode = False
        self.trading_client = None
        self.data_client = None

        # Connection status
        self.connected = False
        self.last_error = None

        # API call tracking
        self.api_call_count = 0
        self.last_api_call = None

        # Initialize clients
        self._initialize_clients()
        
        # CRITICAL: Add simulation mode warnings if detected
        self._validate_trading_mode()

        self.logger.info("Alpaca interface initialized in PRODUCTION mode")

    def _initialize_clients(self):
        """Initialize Alpaca trading and data clients with production validation"""
        if not HAS_ALPACA:
            raise RuntimeError("CRITICAL: Alpaca library not available. Cannot initialize trading interface. Install alpaca-py library.")
        
        try:
            # Create trading client for orders and account management
            self.trading_client = TradingClient(
                api_key=self.config.api_key,
                secret_key=self.config.secret_key,
                paper=True  # Always use paper trading for safety - automatically uses paper endpoint
            )

            # Create data client for market data
            self.data_client = StockHistoricalDataClient(
                api_key=self.config.api_key,
                secret_key=self.config.secret_key
            )

            # Test connection by getting account info
            account = self.trading_client.get_account()
            self.connected = True

            self.logger.info(f"Alpaca API connected successfully")
            self.logger.info(f"Account: {account.account_number[:8]}... | "
                           f"Buying Power: ${float(account.buying_power):,.2f}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Alpaca API: {e}")
            self.connected = False
            raise RuntimeError(f"Alpaca API initialization failed: {e}. Trading unavailable.")

    def _validate_trading_mode(self):
        """Validate trading mode and add critical warnings for simulation mode"""
        # Check if we're using paper trading
        if self.config.base_url and "paper" in self.config.base_url.lower():
            self.logger.warning("PAPER TRADING MODE: Using Alpaca paper trading - trades are simulated but realistic")
            self.logger.warning("PAPER TRADING: Portfolio changes will be tracked but NO REAL MONEY involved")
        
        # Check for test/demo credentials
        if (self.config.api_key in ["test_key", "demo", "test"] or 
            self.config.secret_key in ["test_secret", "demo", "test"]):
            self.logger.error("DEMO CREDENTIALS DETECTED: Using test credentials - NO REAL TRADING POSSIBLE")
            self.logger.error("PRODUCTION WARNING: Replace with real Alpaca API credentials for live trading")
        
        # Check if Alpaca library is missing
        if not HAS_ALPACA:
            self.is_simulation_mode = True
            self.logger.error("CRITICAL: Alpaca library not available - ALL ORDERS WILL BE FAKE")
            self.logger.error("INSTALL REQUIRED: pip install alpaca-py to enable real trading")
        
        # Validate API connectivity
        if not self.connected:
            self.logger.error("NO API CONNECTION: Trading interface not connected to Alpaca")
            self.logger.error("CHECK: Verify API keys and network connectivity")

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
        Submit market order with simulation mode validation

        **Decision Engine Integration:**
        This is called by the execution engine when the risk manager approves a trade.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            quantity: Number of shares
            side: 'buy' or 'sell'

        Returns:
            Tuple[bool, str]: (success, order_id_or_error_message)
        """
        # CRITICAL: Check for simulation mode before submitting order
        if self.is_simulation_mode or not HAS_ALPACA:
            self.logger.error(f"SIMULATION MODE: Cannot submit real order for {side.upper()} {quantity} {symbol}")
            self.logger.error(f"FAKE ORDER: This would be a {side.upper()} order but no real trade executed")
            return False, "SIMULATION_MODE: Order blocked - not connected to real markets"
        
        try:
            self.api_call_count += 1
            self.last_api_call = datetime.now()

            if not self.trading_client:
                return False, "Trading client not available"

            # Add paper trading warning
            if "paper" in self.config.base_url.lower():
                self.logger.warning(f"PAPER TRADING: Submitting {side.upper()} {quantity} {symbol} (simulated trade)")

            # Create market order request
            market_order_data = MarketOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            # Submit order
            order = self.trading_client.submit_order(order_data=market_order_data)

            self.logger.info(f"ORDER SUBMITTED: {side.upper()} {quantity} shares of {symbol} | ID: {order.id}")
            if "paper" in self.config.base_url.lower():
                self.logger.warning(f"PAPER TRADING: Order {order.id} is simulated - no real money involved")
            
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
        Close entire position for symbol with simulation mode validation

        **Emergency Controls:**
        Used by risk manager for emergency position closure.
        """
        # CRITICAL: Check for simulation mode before closing position
        if self.is_simulation_mode or not HAS_ALPACA:
            self.logger.error(f"SIMULATION MODE: Cannot close real position for {symbol}")
            self.logger.error(f"FAKE CLOSE: This would close {symbol} position but no real trade executed")
            return False, "SIMULATION_MODE: Position close blocked - not connected to real markets"
        
        try:
            if not self.trading_client:
                return False, "Trading client not available"

            # Add paper trading warning
            if "paper" in self.config.base_url.lower():
                self.logger.warning(f"PAPER TRADING: Closing {symbol} position (simulated trade)")

            # Close the position
            close_response = self.trading_client.close_position(symbol)

            self.logger.info(f"POSITION CLOSED: {symbol}")
            if "paper" in self.config.base_url.lower():
                self.logger.warning(f"PAPER TRADING: Position close is simulated - no real money involved")
            
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
            'is_connected': self.connected,
            'is_simulation_mode': self.is_simulation_mode,
            'api_call_count': self.api_call_count,
            'last_api_call': self.last_api_call.isoformat() if self.last_api_call else None,
            'has_alpaca_library': HAS_ALPACA,
            'config_loaded': bool(self.config.api_key and self.config.secret_key),
            'trading_mode': 'PAPER' if 'paper' in self.config.base_url.lower() else 'UNKNOWN',
            'ready_for_production': self._is_ready_for_production()
        }
    
    def _is_ready_for_production(self) -> bool:
        """Check if interface is ready for production trading"""
        if not HAS_ALPACA:
            return False
        if not self.connected:
            return False
        if self.is_simulation_mode:
            return False
        if not self.config.api_key or not self.config.secret_key:
            return False
        if self.config.api_key in ["test_key", "demo", "test"]:
            return False
        return True

    def reconnect(self) -> bool:
        """
        Attempt to reconnect to Alpaca API

        **Resilience:**
        Called by monitoring processes when connection issues are detected.
        """
        self.logger.info("Attempting to reconnect to Alpaca API...")
        self.connected = False

        if HAS_ALPACA and self.config.api_key and self.config.secret_key:
            self._initialize_clients()
            return self.connected
        else:
            self.is_simulation_mode = True
            self.connected = True  # Consider simulation as "connected"
            self.logger.warning("CRITICAL WARNING: Running in SIMULATION MODE - NO REAL TRADES WILL BE EXECUTED")
            self.logger.warning("SIMULATION MODE: All orders will be fake - system is NOT connected to real markets")
            self.logger.warning("TO ENABLE REAL TRADING: Install alpaca-py library and provide valid API credentials")
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

