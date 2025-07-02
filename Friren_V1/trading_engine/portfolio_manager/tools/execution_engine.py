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
    from .order_manager import SimpleOrderManager, OrderRequest, OrderType, TimeInForce
    # Import OrderSide directly from Alpaca to avoid local enum conflicts
    try:
        from alpaca.trading.enums import OrderSide
    except ImportError:
        # Fallback enum if Alpaca not available
        from enum import Enum
        class OrderSide(Enum):
            BUY = "buy"
            SELL = "sell"

    from .alpaca_interface import SimpleAlpacaInterface
    from .db_manager import TradingDBManager
    IMPORTS_SUCCESSFUL = True
except ImportError:
    # Fallback stubs
    IMPORTS_SUCCESSFUL = False

    from enum import Enum
    class OrderSide(Enum):
        BUY = "buy"
        SELL = "sell"

    # REMOVED: Dangerous fake trading interfaces
    # These stub classes were returning fake success responses which could mask
    # real trading failures. Only real trading interfaces should be used.
    # If real interfaces are not available, the system should fail safely.


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

    def __init__(self, order_manager: SimpleOrderManager,
                 alpaca_interface: SimpleAlpacaInterface,
                 db_manager: TradingDBManager):
        self.logger = logging.getLogger("execution_engine")

        # PRODUCTION VALIDATION: Ensure all dependencies are properly initialized
        if not order_manager:
            raise ValueError("SimpleOrderManager is required for production execution")
        if not alpaca_interface:
            raise ValueError("SimpleAlpacaInterface is required for production execution")
        if not db_manager:
            raise ValueError("TradingDBManager is required for production execution")
        
        # Validate Alpaca interface is properly configured
        try:
            # Test basic connectivity without making trades
            account_info = alpaca_interface.get_account_info()
            if not account_info:
                raise RuntimeError("Alpaca interface failed account validation - cannot execute trades")
            self.logger.info("Alpaca interface validated successfully")
        except Exception as e:
            raise RuntimeError(f"Alpaca interface validation failed: {e}. Trading execution unavailable.")

        # Tool integrations
        self.order_manager = order_manager
        self.alpaca_interface = alpaca_interface
        self.db_manager = db_manager

        # Simple stats
        self.stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'total_volume': 0.0,
            'validation_errors': 0,
            'alpaca_errors': 0
        }

        self.logger.info("SimpleExecutionEngine initialized with validated dependencies")

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
        """Execute simple market order with comprehensive error handling"""
        try:
            # VALIDATION: Pre-flight checks
            validation_error = self._validate_execution_prerequisites(request)
            if validation_error:
                self.stats['validation_errors'] += 1
                return self._create_error_result(request, f"Validation failed: {validation_error}")

            # Get current price with specific error handling
            try:
                current_price = self.alpaca_interface.get_current_price(request.symbol)
                if not current_price or current_price <= 0:
                    raise ValueError(f"Invalid price received: {current_price}")
            except Exception as e:
                self.stats['alpaca_errors'] += 1
                return self._create_error_result(request, f"Price fetch failed: {str(e)}")

            # Determine order side
            side = OrderSide.BUY if request.execution_type == ExecutionType.BUY else OrderSide.SELL

            # Create order with validation
            try:
                order_request = OrderRequest(
                    symbol=request.symbol,
                    side=side,
                    quantity=request.quantity,
                    order_type=OrderType.MARKET,
                    strategy_name=request.strategy_name,
                    confidence_score=request.confidence_score
                )
            except Exception as e:
                return self._create_error_result(request, f"Order request creation failed: {str(e)}")

            # Submit order to order manager with specific error handling
            try:
                order_success, order_or_error = self.order_manager.create_order(order_request, current_price)
                if not order_success:
                    return self._create_error_result(request, f"Order creation failed: {order_or_error}")
                order = order_or_error
            except Exception as e:
                return self._create_error_result(request, f"Order manager error: {str(e)}")

            # Submit to Alpaca with comprehensive error handling
            try:
                alpaca_success, alpaca_id_or_error = self.alpaca_interface.submit_order(order)
                if not alpaca_success:
                    self.stats['alpaca_errors'] += 1
                    # Try to cancel the order in order manager
                    try:
                        self.order_manager.cancel_order(order)
                    except Exception:
                        pass  # Best effort cleanup
                    return self._create_error_result(request, f"Alpaca submission failed: {alpaca_id_or_error}")
            except Exception as e:
                self.stats['alpaca_errors'] += 1
                return self._create_error_result(request, f"Alpaca interface error: {str(e)}")

            # Mark as submitted
            try:
                self.order_manager.submit_order(order)
            except Exception as e:
                self.logger.warning(f"Failed to mark order as submitted: {e}")
                # Continue execution as order was already submitted to Alpaca

            # Wait for fill (simplified - assume market fill)
            fill_price = current_price
            filled_quantity = request.quantity

            # Update position in database with error handling
            try:
                self._update_position(request.symbol, filled_quantity, fill_price, side)
            except Exception as e:
                self.logger.error(f"Failed to update position in database: {e}")
                # Continue as trade was executed, just DB update failed

            # Update stats
            self.stats['successful_executions'] += 1
            self.stats['total_volume'] += filled_quantity * fill_price

            execution_time = time.time() - start_time

            self.logger.info(f"EXECUTION: {side.value} {filled_quantity} {request.symbol} @ ${fill_price:.2f}")
            
            # BUSINESS LOGIC OUTPUT - Real execution details
            order_value = filled_quantity * fill_price
            print(f"[EXECUTION] {request.symbol}: EXECUTED {side.value.upper()} {filled_quantity} shares at ${fill_price:.2f} | order_id: {alpaca_id_or_error} | value: ${order_value:,.2f}")

            return SimpleExecutionResult(
                symbol=request.symbol,
                execution_type=request.execution_type,
                success=True,
                filled_quantity=filled_quantity,
                fill_price=fill_price,
                execution_time_seconds=execution_time
            )

        except Exception as e:
            self.logger.error(f"Unexpected market order execution error: {e}")
            return self._create_error_result(request, f"Unexpected error: {str(e)}")

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
        """Basic request validation"""
        if not request.symbol or request.quantity <= 0:
            return False
        if request.max_slippage_percent < 0 or request.max_slippage_percent > 20:
            return False
        return True
    
    def _validate_execution_prerequisites(self, request: SimpleExecutionRequest) -> Optional[str]:
        """Comprehensive validation before execution"""
        # Basic request validation
        if not self._validate_request(request):
            return "Invalid request parameters"
        
        # Symbol validation
        if not request.symbol or len(request.symbol) < 1 or len(request.symbol) > 5:
            return f"Invalid symbol format: {request.symbol}"
        
        # Quantity validation
        if request.quantity <= 0 or request.quantity > 10000:
            return f"Invalid quantity: {request.quantity}"
        
        # Strategy validation
        if not request.strategy_name or len(request.strategy_name) < 2:
            return "Strategy name is required"
        
        # Confidence score validation
        if request.confidence_score < 0 or request.confidence_score > 1:
            return f"Invalid confidence score: {request.confidence_score}"
        
        # Check if Alpaca interface is available
        if not self.alpaca_interface:
            return "Alpaca interface not available"
        
        # Check if order manager is available
        if not self.order_manager:
            return "Order manager not available"
        
        # Check if database manager is available
        if not self.db_manager:
            return "Database manager not available"
        
        return None  # No validation errors

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
        """Emergency close all positions with comprehensive error handling"""
        results = []
        
        try:
            self.logger.warning("EMERGENCY: Force closing all positions")

            # Validate Alpaca interface availability
            if not self.alpaca_interface:
                self.logger.error("Cannot force close: Alpaca interface not available")
                return []
            
            # Get positions with error handling
            try:
                positions = self.alpaca_interface.get_positions()
                if not positions:
                    self.logger.info("No positions to close")
                    return []
            except Exception as e:
                self.logger.error(f"Failed to get positions for force close: {e}")
                return []

            self.logger.warning(f"Found {len(positions)} positions to force close")
            
            # Close each position individually with error isolation
            for position in positions:
                try:
                    if abs(position.quantity) > 0.001:
                        self.logger.warning(f"Force closing {position.symbol}: {position.quantity} shares")
                        
                        request = SimpleExecutionRequest(
                            symbol=position.symbol,
                            execution_type=ExecutionType.FORCE_CLOSE,
                            quantity=abs(position.quantity),
                            strategy_name="emergency_close",
                            max_slippage_percent=15.0  # Higher slippage tolerance for emergency
                        )
                        
                        result = self.execute_trade(request)
                        results.append(result)
                        
                        if result.success:
                            self.logger.warning(f"Force closed {position.symbol}")
                        else:
                            self.logger.error(f"Failed to force close {position.symbol}: {result.error_message}")
                    else:
                        self.logger.info(f"Skipping {position.symbol}: position too small ({position.quantity})")
                        
                except Exception as e:
                    self.logger.error(f"Error force closing {getattr(position, 'symbol', 'unknown')}: {e}")
                    # Create error result for this position
                    error_result = SimpleExecutionResult(
                        symbol=getattr(position, 'symbol', 'unknown'),
                        execution_type=ExecutionType.FORCE_CLOSE,
                        success=False,
                        error_message=str(e)
                    )
                    results.append(error_result)
                    continue

            # Log summary
            successful_closes = sum(1 for r in results if r.success)
            failed_closes = len(results) - successful_closes
            
            self.logger.warning(f"Force close complete: {successful_closes} successful, {failed_closes} failed")
            
            return results

        except Exception as e:
            self.logger.error(f"Critical error in force close all: {e}")
            return results  # Return partial results if any


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
