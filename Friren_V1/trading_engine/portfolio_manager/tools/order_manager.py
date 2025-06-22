"""
trading_engine/portfolio_manager.py/tools/order_manager.py
"""

import uuid
import time
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from decimal import Decimal
import logging
import json
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import Alpaca enums (with fallback)
try:
    from alpaca.trading.enums import OrderSide, OrderType as AlpacaOrderType, TimeInForce
    HAS_ALPACA = True
except ImportError:
    # Create enum stubs for development when Alpaca not available
    class OrderSide(Enum):
        BUY = "buy"
        SELL = "sell"

    class AlpacaOrderType(Enum):
        MARKET = "market"
        LIMIT = "limit"

    class TimeInForce(Enum):
        DAY = "day"
        GTC = "gtc"

    HAS_ALPACA = False

try:
    # Try absolute imports first
    from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
    from Friren_V1.trading_engine.portfolio_manager.tools.position_sizer import PurePositionSizer
except ImportError:
        # Fallback stubs
        class TradingDBManager:
            def __init__(self, process_name: str = "order_manager"):
                self.logger = logging.getLogger(f"db.{process_name}")
            def insert_transaction(self, **kwargs): return True
            def get_holdings(self, **kwargs): return []

        class PurePositionSizer:
            def calculate_position_size(self, **kwargs): return 1000.0


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class TimeInForce(Enum):
    DAY = "day"
    GTC = "gtc"


@dataclass
class OrderRequest:
    """Simple order request"""
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.DAY
    strategy_name: Optional[str] = None
    confidence_score: Optional[float] = None


@dataclass
class Order:
    """Simple order tracking"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    status: OrderStatus
    limit_price: Optional[float] = None
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.now)
    broker_order_id: Optional[str] = None
    strategy_name: Optional[str] = None
    confidence_score: Optional[float] = None


class SimpleOrderManager:
    """Simplified Order Manager with fixed imports"""

    def __init__(self, db_manager: Optional[TradingDBManager] = None):
        self.logger = logging.getLogger("order_manager")
        self.db_manager = db_manager or TradingDBManager("order_manager")
        self.active_orders: Dict[str, Order] = {}
        self.stats = {'orders_created': 0, 'orders_filled': 0}

    def create_order(self, order_request: OrderRequest,
                    current_price: Optional[float] = None) -> Tuple[bool, Union[Order, str]]:
        """Create order with validation"""
        try:
            # Basic validation
            if not order_request.symbol or order_request.quantity <= 0:
                return False, "Invalid order request"

            # Create order
            order = Order(
                order_id=str(uuid.uuid4()),
                symbol=order_request.symbol.upper(),
                side=order_request.side,
                quantity=order_request.quantity,
                order_type=order_request.order_type,
                status=OrderStatus.PENDING,
                limit_price=order_request.limit_price,
                strategy_name=order_request.strategy_name,
                confidence_score=order_request.confidence_score
            )

            # Store order
            self.active_orders[order.order_id] = order
            self.stats['orders_created'] += 1

            self.logger.info(f"Created order {order.order_id}: {order.side.value} {order.quantity} {order.symbol}")
            return True, order

        except Exception as e:
            return False, f"Error creating order: {e}"

    def submit_order(self, order: Order) -> bool:
        """Mark order as submitted"""
        try:
            order.status = OrderStatus.SUBMITTED
            self.logger.info(f"Submitted order {order.order_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error submitting order: {e}")
            return False

    def update_order_fill(self, order_id: str, filled_quantity: float,
                         fill_price: float, broker_order_id: Optional[str] = None) -> bool:
        """Update order with fill info"""
        try:
            order = self.active_orders.get(order_id)
            if not order:
                return False

            order.filled_quantity = filled_quantity
            order.average_fill_price = fill_price
            if broker_order_id:
                order.broker_order_id = broker_order_id

            if filled_quantity >= order.quantity:
                order.status = OrderStatus.FILLED
                self.stats['orders_filled'] += 1

            # Record in database
            quantity_signed = filled_quantity if order.side == OrderSide.BUY else -filled_quantity
            self.db_manager.insert_transaction(
                symbol=order.symbol,
                quantity=quantity_signed,
                price=fill_price,
                timestamp=datetime.now(),
                order_id=order_id,
                confidence_score=order.confidence_score
            )

            return True

        except Exception as e:
            self.logger.error(f"Error updating fill: {e}")
            return False
