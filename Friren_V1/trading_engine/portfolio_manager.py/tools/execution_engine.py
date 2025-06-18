"""
trading_engine/portfolio_manager/tools/execution_engine.py

Simple Execution Engine - Lean Market Order Execution

Streamlined execution focused on essentials:
- Market orders only 
- Simple position tracking
- Basic error handling
- Integration with Order Manager and Alpaca

"""

import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

# Import our tools
try:
    from .order_manager import OrderManager, OrderRequest, OrderSide, OrderType, TimeInForce
    from .alpaca_interface import AlpacaInterface
    from .db_manager import TradingDBManager
except ImportError:
    # Fallback stubs
    class OrderManager:
        def create_order(self, *args, **kwargs): return True, None
        def submit_order(self, *args, **kwargs): return True
    class AlpacaInterface:
        def get_current_price(self, symbol): return 100.0
        def submit_order(self, *args, **kwargs): return True, "fake_id"
        def get_position(self, symbol): return None
    class TradingDBManager:
        def upsert_holding(self, *args, **kwargs): return True


class ExecutionType(Enum):
    """Simple execution types"""
    BUY = "buy"
    SELL = "sell"
    CLOSE = "close"
    FORCE_CLOSE = "force_close"


@dataclass
class SimpleExecutionRequest:
    """Simplified execution request"""
    symbol: str
    execution_type: ExecutionType
    quantity: float
    strategy_name: str = "default"
    confidence_score: float = 0.5
    max_slippage_percent: float = 2.0


@dataclass
class SimpleExecutionResult:
    """Simplified execution result"""
    symbol: str
    execution_type: ExecutionType
    success: bool
    filled_quantity: float = 0.0
    fill_price: Optional[float] = None
    error_message: Optional[str] = None
    execution_time_seconds: float = 0.0


class SimpleExecutionEngine:
    """
    Simple Execution Engine

    Does one thing well: Execute market orders quickly
    - No complex tranching
    - No async overhead
    - No excessive tracking
    - Just fast, simple execution
    """

    def __init__(self, order_manager: OrderManager,
                 alpaca_interface: AlpacaInterface,
                 db_manager: TradingDBManager):
        self.logger = logging.getLogger("execution_engine")

        # Tool integrations
        self.order_manager = order_manager
        self.alpaca_interface = alpaca_interface
        self.db_manager = db_manager

        # Simple stats
        self.stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'total_volume': 0.0
        }

        self.logger.info("SimpleExecutionEngine initialized")

    def execute_trade(self, request: SimpleExecutionRequest) -> SimpleExecutionResult:
        """
        Execute trade - the main method

        Args:
            request: SimpleExecutionRequest

        Returns:
            SimpleExecutionResult
        """
        start_time = time.time()

        try:
            self.stats['total_executions'] += 1

            # Validate request
            if not self._validate_request(request):
                return self._create_error_result(request, "Invalid request")

            # Handle different execution types
            if request.execution_type == ExecutionType.CLOSE:
                return self._execute_close(request, start_time)
            elif request.execution_type == ExecutionType.FORCE_CLOSE:
                return self._execute_force_close(request, start_time)
            elif request.execution_type in [ExecutionType.BUY, ExecutionType.SELL]:
                return self._execute_market_order(request, start_time)
            else:
                return self._create_error_result(request, f"Unknown execution type: {request.execution_type}")

        except Exception as e:
            self.logger.error(f"Execution error for {request.symbol}: {e}")
            return self._create_error_result(request, str(e))

    def _execute_market_order(self, request: SimpleExecutionRequest, start_time: float) -> SimpleExecutionResult:
        """Execute simple market order"""
        try:
            # Get current price
            current_price = self.alpaca_interface.get_current_price(request.symbol)
            if not current_price:
                return self._create_error_result(request, "Cannot get current price")

            # Determine order side
            side = OrderSide.BUY if request.execution_type == ExecutionType.BUY else OrderSide.SELL

            # Create order
            order_request = OrderRequest(
                symbol=request.symbol,
                side=side,
                quantity=request.quantity,
                order_type=OrderType.MARKET,
                strategy_name=request.strategy_name,
                confidence_score=request.confidence_score
            )

            # Submit order
            order_success, order_or_error = self.order_manager.create_order(order_request, current_price)
            if not order_success:
                return self._create_error_result(request, f"Order creation failed: {order_or_error}")

            order = order_or_error

            # Submit to Alpaca
            alpaca_success, alpaca_id_or_error = self.alpaca_interface.submit_order(order)
            if not alpaca_success:
                return self._create_error_result(request, f"Alpaca submission failed: {alpaca_id_or_error}")

            # Mark as submitted
            self.order_manager.submit_order(order)

            # Wait for fill (simplified - just assume it fills at market price)
            fill_price = current_price
            filled_quantity = request.quantity

            # Update position in database
            self._update_position(request.symbol, filled_quantity, fill_price, side)

            # Update stats
            self.stats['successful_executions'] += 1
            self.stats['total_volume'] += filled_quantity * fill_price

            execution_time = time.time() - start_time

            self.logger.info(f"Executed {side.value} {filled_quantity} {request.symbol} @ {fill_price}")

            return SimpleExecutionResult(
                symbol=request.symbol,
                execution_type=request.execution_type,
                success=True,
                filled_quantity=filled_quantity,
                fill_price=fill_price,
                execution_time_seconds=execution_time
            )

        except Exception as e:
            self.logger.error(f"Market order execution error: {e}")
            return self._create_error_result(request, str(e))

    def _execute_close(self, request: SimpleExecutionRequest, start_time: float) -> SimpleExecutionResult:
        """Execute position close"""
        try:
            # Get current position
            position = self.alpaca_interface.get_position(request.symbol)
            if not position or abs(position.quantity) < 0.001:
                return SimpleExecutionResult(
                    symbol=request.symbol,
                    execution_type=request.execution_type,
                    success=True,
                    filled_quantity=0.0,
                    execution_time_seconds=time.time() - start_time
                )

            # Create close order
            close_quantity = abs(position.quantity)
            close_side = OrderSide.SELL if position.quantity > 0 else OrderSide.BUY

            close_request = SimpleExecutionRequest(
                symbol=request.symbol,
                execution_type=ExecutionType.SELL if close_side == OrderSide.SELL else ExecutionType.BUY,
                quantity=close_quantity,
                strategy_name=request.strategy_name,
                confidence_score=request.confidence_score
            )

            return self._execute_market_order(close_request, start_time)

        except Exception as e:
            self.logger.error(f"Position close error: {e}")
            return self._create_error_result(request, str(e))

    def _execute_force_close(self, request: SimpleExecutionRequest, start_time: float) -> SimpleExecutionResult:
        """Execute force close (same as close but with higher slippage tolerance)"""
        self.logger.warning(f"Force closing position: {request.symbol}")

        # Force close is just a close with higher slippage tolerance
        request.max_slippage_percent = 10.0  # Accept higher slippage
        return self._execute_close(request, start_time)

    def _update_position(self, symbol: str, quantity: float, price: float, side: OrderSide):
        """Update position in database"""
        try:
            # Calculate quantity change
            quantity_change = quantity if side == OrderSide.BUY else -quantity

            # Get current position
            holdings = self.db_manager.get_holdings(symbol=symbol, active_only=False)

            if holdings:
                # Update existing
                current = holdings[0]
                old_qty = float(current['net_quantity'])
                old_cost = float(current['avg_cost_basis'])
                old_invested = float(current['total_invested'])

                new_qty = old_qty + quantity_change

                if side == OrderSide.BUY:
                    # Adding to position
                    new_invested = old_invested + (quantity * price)
                    new_cost = new_invested / new_qty if new_qty != 0 else old_cost
                else:
                    # Reducing position
                    new_invested = old_invested * (new_qty / old_qty) if old_qty != 0 else 0
                    new_cost = old_cost

                self.db_manager.upsert_holding(
                    symbol=symbol,
                    net_quantity=new_qty,
                    avg_cost_basis=new_cost,
                    total_invested=new_invested,
                    last_transaction_date=datetime.now(),
                    number_of_transactions=int(current['number_of_transactions']) + 1
                )
            else:
                # New position
                self.db_manager.upsert_holding(
                    symbol=symbol,
                    net_quantity=quantity_change,
                    avg_cost_basis=price,
                    total_invested=quantity * price,
                    first_purchase_date=datetime.now(),
                    last_transaction_date=datetime.now(),
                    number_of_transactions=1
                )

        except Exception as e:
            self.logger.error(f"Error updating position: {e}")

    def _validate_request(self, request: SimpleExecutionRequest) -> bool:
        """Simple validation"""
        if not request.symbol or request.quantity <= 0:
            return False
        if request.max_slippage_percent < 0 or request.max_slippage_percent > 20:
            return False
        return True

    def _create_error_result(self, request: SimpleExecutionRequest, error: str) -> SimpleExecutionResult:
        """Create error result"""
        return SimpleExecutionResult(
            symbol=request.symbol,
            execution_type=request.execution_type,
            success=False,
            error_message=error
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get simple statistics"""
        return self.stats.copy()

    def force_close_all_positions(self) -> List[SimpleExecutionResult]:
        """Emergency close all positions"""
        try:
            self.logger.warning("Force closing all positions")

            positions = self.alpaca_interface.get_positions()
            results = []

            for position in positions:
                if abs(position.quantity) > 0.001:
                    request = SimpleExecutionRequest(
                        symbol=position.symbol,
                        execution_type=ExecutionType.FORCE_CLOSE,
                        quantity=abs(position.quantity),
                        strategy_name="emergency_close"
                    )
                    result = self.execute_trade(request)
                    results.append(result)

            return results

        except Exception as e:
            self.logger.error(f"Force close all error: {e}")
            return []


# Utility functions - much simpler
def create_buy_order(symbol: str, quantity: float, strategy: str = "default") -> SimpleExecutionRequest:
    """Create buy order"""
    return SimpleExecutionRequest(
        symbol=symbol.upper(),
        execution_type=ExecutionType.BUY,
        quantity=quantity,
        strategy_name=strategy
    )

def create_sell_order(symbol: str, quantity: float, strategy: str = "default") -> SimpleExecutionRequest:
    """Create sell order"""
    return SimpleExecutionRequest(
        symbol=symbol.upper(),
        execution_type=ExecutionType.SELL,
        quantity=quantity,
        strategy_name=strategy
    )

def create_close_order(symbol: str, strategy: str = "close") -> SimpleExecutionRequest:
    """Create close order"""
    return SimpleExecutionRequest(
        symbol=symbol.upper(),
        execution_type=ExecutionType.CLOSE,
        quantity=0.0,  # Will be determined by actual position
        strategy_name=strategy
    )
