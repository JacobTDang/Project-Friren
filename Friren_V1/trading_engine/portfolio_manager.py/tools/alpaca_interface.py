"""
trading_engine/portfolio_manager.py/tools/alpaca_interface.py
"""

import os
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

# Alpaca imports with fallback
try:
    import alpaca_trade_api as tradeapi
    HAS_ALPACA = True
except ImportError:
    HAS_ALPACA = False
    logging.warning("Alpaca API not available")


@dataclass
class AlpacaConfig:
    """Alpaca configuration"""
    api_key: str
    secret_key: str
    base_url: str = "https://paper-api.alpaca.markets"

    @classmethod
    def from_environment(cls):
        return cls(
            api_key=os.getenv("ALPACA_API_KEY", ""),
            secret_key=os.getenv("ALPACA_SECRET_KEY", "")
        )


@dataclass
class AlpacaAccount:
    """Account info"""
    account_id: str
    buying_power: float
    cash: float
    portfolio_value: float


@dataclass
class AlpacaPosition:
    """Position info"""
    symbol: str
    quantity: float
    market_value: float
    current_price: float


class SimpleAlpacaInterface:
    """Simplified Alpaca interface"""

    def __init__(self, config: Optional[AlpacaConfig] = None):
        self.logger = logging.getLogger("alpaca_interface")
        self.config = config or AlpacaConfig.from_environment()
        self.api_client = None
        self.is_connected = False

        if HAS_ALPACA:
            self._initialize_api()
        else:
            self.logger.warning("Running in simulation mode")
            self.is_connected = True

    def _initialize_api(self):
        """Initialize Alpaca API"""
        try:
            self.api_client = tradeapi.REST(
                key_id=self.config.api_key,
                secret_key=self.config.secret_key,
                base_url=self.config.base_url
            )
            self.is_connected = True
            self.logger.info("Alpaca API initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize Alpaca: {e}")

    def submit_order(self, order: Order) -> Tuple[bool, str]:
        """Submit order to Alpaca"""
        try:
            if not HAS_ALPACA:
                fake_id = f"sim_{order.order_id[:8]}"
                self.logger.info(f"Simulated order submission: {fake_id}")
                return True, fake_id

            # Submit to real Alpaca
            alpaca_order = self.api_client.submit_order(
                symbol=order.symbol,
                qty=order.quantity,
                side=order.side.value,
                type=order.order_type.value,
                time_in_force=TimeInForce.DAY.value
            )

            return True, alpaca_order.id

        except Exception as e:
            return False, str(e)

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price"""
        try:
            if not HAS_ALPACA:
                return 100.0 + hash(symbol) % 100  # Fake price

            latest_trade = self.api_client.get_latest_trade(symbol)
            return float(latest_trade.price) if latest_trade else None

        except Exception as e:
            self.logger.error(f"Error getting price: {e}")
            return None

    def get_position(self, symbol: str) -> Optional[AlpacaPosition]:
        """Get position for symbol"""
        try:
            if not HAS_ALPACA:
                return None

            position = self.api_client.get_position(symbol)
            return AlpacaPosition(
                symbol=position.symbol,
                quantity=float(position.qty),
                market_value=float(position.market_value),
                current_price=float(position.current_price)
            ) if position else None

        except Exception:
            return None

    def get_positions(self) -> List[AlpacaPosition]:
        """Get all positions"""
        try:
            if not HAS_ALPACA:
                return []

            positions = self.api_client.list_positions()
            return [AlpacaPosition(
                symbol=pos.symbol,
                quantity=float(pos.qty),
                market_value=float(pos.market_value),
                current_price=float(pos.current_price)
            ) for pos in positions]

        except Exception as e:
            self.logger.error(f"Error getting positions: {e}")
            return []
