"""
trading_engine/portfolio_manager.py/tools/position_sizer.py

Lean Position Sizer - Pure Sizing with Database Integration

Focuses ONLY on position sizing calculations:
- How much to buy/sell based on confidence and regime
- Current position awareness (from database)
- Risk-based size adjustments
- Portfolio allocation limits (15% max per symbol)

Decision engine makes the decisions, position sizer just calculates sizes.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
import sys
import os

# Fixed imports for your structure
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from .db_manager import TradingDBManager
    from .alpaca_interface import SimpleAlpacaInterface
except ImportError:
    # Fallback stubs
    class TradingDBManager:
        def get_holdings(self, **kwargs): return []
    class SimpleAlpacaInterface:
        def get_account_info(self): return None


class SizeAction(Enum):
    """What type of sizing action"""
    OPEN_POSITION = "open_position"     # New position
    ADD_TO_POSITION = "add_to_position" # Increase existing position
    REDUCE_POSITION = "reduce_position" # Decrease existing position
    CLOSE_POSITION = "close_position"   # Close entire position


@dataclass
class PositionSizeResult:
    """Result of position sizing calculation"""
    symbol: str
    action: SizeAction
    target_size_pct: float      # Target position size as % of portfolio
    current_size_pct: float     # Current position size as % of portfolio
    size_change_pct: float      # Change needed (+ = increase, - = decrease)

    # Dollar amounts
    target_dollar_amount: float
    current_dollar_amount: float
    trade_dollar_amount: float  # Actual $ amount to trade

    # Shares
    shares_to_trade: float      # Number of shares to buy/sell
    current_shares: float       # Current shares owned

    # Reasoning
    confidence_score: float
    volatility_adjustment: float
    regime_adjustment: float
    sentiment_adjustment: float
    portfolio_limit_applied: bool
    reasoning: str

    # Risk info
    risk_warnings: List[str]
    timestamp: datetime

    @property
    def should_trade(self) -> bool:
        """Whether this position needs a trade"""
        return abs(self.size_change_pct) > 0.01 and abs(self.trade_dollar_amount) > 100

    @property
    def trade_direction(self) -> str:
        """BUY or SELL"""
        return "BUY" if self.size_change_pct > 0 else "SELL"


class LeanPositionSizer:
    """
    Lean Position Sizer - Pure sizing calculations

    Philosophy:
    - Only calculates position sizes
    - Integrates with database for current positions
    - Applies portfolio limits and risk adjustments
    - Does NOT make trading decisions
    """

    def __init__(self, db_manager: Optional[TradingDBManager] = None,
                 alpaca_interface: Optional[SimpleAlpacaInterface] = None):
        self.logger = logging.getLogger("position_sizer")

        # Tool integrations
        self.db_manager = db_manager or TradingDBManager("position_sizer")
        self.alpaca_interface = alpaca_interface

        # Configuration
        self.config = {
            # Position limits
            'max_position_size_pct': 0.15,      # 15% max per symbol
            'min_position_size_pct': 0.02,      # 2% minimum
            'max_total_allocation_pct': 0.80,   # 80% max total allocation

            # Base sizing
            'base_position_size_pct': 0.05,     # 5% base size
            'confidence_scaling_factor': 2.0,   # How much confidence affects size

            # Risk adjustments
            'high_volatility_threshold': 0.30,  # 30% annualized volatility
            'volatility_reduction_factor': 0.5, # Reduce size by 50% for high vol

            # Regime adjustments
            'regime_multipliers': {
                'BULL_MARKET': 1.2,
                'BEAR_MARKET': 0.7,
                'HIGH_VOLATILITY': 0.6,
                'LOW_VOLATILITY': 1.1,
                'SIDEWAYS': 0.9,
                'UNKNOWN': 1.0
            },

            # Sentiment adjustments
            'sentiment_multipliers': {
                'extreme_positive': 1.3,  # +0.7 to +1.0 sentiment
                'positive': 1.1,          # +0.3 to +0.7 sentiment
                'neutral': 1.0,           # -0.3 to +0.3 sentiment
                'negative': 0.8,          # -0.7 to -0.3 sentiment
                'extreme_negative': 0.5   # -1.0 to -0.7 sentiment
            },

            # Rebalancing
            'rebalance_threshold_pct': 0.02,    # 2% threshold for rebalancing
            'min_trade_amount': 100.0            # $100 minimum trade
        }

        self.logger.info("LeanPositionSizer initialized")

    def calculate_position_size(self, symbol: str, confidence_score: float,
                              sentiment_score: float = 0.0, market_regime: str = "UNKNOWN",
                              volatility: Optional[float] = None,
                              current_price: Optional[float] = None) -> PositionSizeResult:
        """
        Calculate position size for a symbol

        Args:
            symbol: Stock symbol
            confidence_score: Decision confidence (0.0 to 1.0)
            sentiment_score: Sentiment score (-1.0 to +1.0)
            market_regime: Current market regime
            volatility: Estimated volatility (optional)
            current_price: Current stock price (optional)

        Returns:
            PositionSizeResult with sizing details
        """
        try:
            # Get current portfolio state
            portfolio_value = self._get_portfolio_value()
            current_position = self._get_current_position(symbol)
            current_size_pct = current_position['size_pct']
            current_dollar_amount = current_position['dollar_amount']
            current_shares = current_position['shares']

            # Calculate base target size
            base_size_pct = self._calculate_base_size(confidence_score)

            # Apply adjustments
            adjusted_size_pct = self._apply_adjustments(
                base_size_pct, sentiment_score, market_regime, volatility
            )

            # Apply portfolio limits
            final_size_pct, limit_applied = self._apply_portfolio_limits(
                adjusted_size_pct, symbol, portfolio_value
            )

            # Calculate trade details
            size_change_pct = final_size_pct - current_size_pct
            target_dollar_amount = final_size_pct * portfolio_value
            trade_dollar_amount = size_change_pct * portfolio_value

            # Determine action type
            action = self._determine_action(current_size_pct, final_size_pct, size_change_pct)

            # Calculate shares to trade
            shares_to_trade = self._calculate_shares_to_trade(
                trade_dollar_amount, current_price or 100.0
            )

            # Generate reasoning and warnings
            reasoning, warnings = self._generate_reasoning_and_warnings(
                symbol, base_size_pct, final_size_pct, confidence_score,
                sentiment_score, market_regime, limit_applied
            )

            result = PositionSizeResult(
                symbol=symbol,
                action=action,
                target_size_pct=final_size_pct,
                current_size_pct=current_size_pct,
                size_change_pct=size_change_pct,
                target_dollar_amount=target_dollar_amount,
                current_dollar_amount=current_dollar_amount,
                trade_dollar_amount=trade_dollar_amount,
                shares_to_trade=abs(shares_to_trade),
                current_shares=current_shares,
                confidence_score=confidence_score,
                volatility_adjustment=self._get_volatility_adjustment(volatility),
                regime_adjustment=self.config['regime_multipliers'].get(market_regime, 1.0),
                sentiment_adjustment=self._get_sentiment_adjustment(sentiment_score),
                portfolio_limit_applied=limit_applied,
                reasoning=reasoning,
                risk_warnings=warnings,
                timestamp=datetime.now()
            )

            self.logger.debug(f"Position sizing for {symbol}: {final_size_pct:.1%} "
                            f"(change: {size_change_pct:+.1%})")

            return result

        except Exception as e:
            self.logger.error(f"Error calculating position size for {symbol}: {e}")
            # Return safe default
            return self._create_safe_default_result(symbol, confidence_score)

    def calculate_close_size(self, symbol: str) -> PositionSizeResult:
        """Calculate size to close entire position"""
        try:
            current_position = self._get_current_position(symbol)
            portfolio_value = self._get_portfolio_value()

            if current_position['shares'] == 0:
                # No position to close
                return PositionSizeResult(
                    symbol=symbol,
                    action=SizeAction.CLOSE_POSITION,
                    target_size_pct=0.0,
                    current_size_pct=0.0,
                    size_change_pct=0.0,
                    target_dollar_amount=0.0,
                    current_dollar_amount=0.0,
                    trade_dollar_amount=0.0,
                    shares_to_trade=0.0,
                    current_shares=0.0,
                    confidence_score=1.0,
                    volatility_adjustment=1.0,
                    regime_adjustment=1.0,
                    sentiment_adjustment=1.0,
                    portfolio_limit_applied=False,
                    reasoning="No position to close",
                    risk_warnings=[],
                    timestamp=datetime.now()
                )

            # Close entire position
            return PositionSizeResult(
                symbol=symbol,
                action=SizeAction.CLOSE_POSITION,
                target_size_pct=0.0,
                current_size_pct=current_position['size_pct'],
                size_change_pct=-current_position['size_pct'],
                target_dollar_amount=0.0,
                current_dollar_amount=current_position['dollar_amount'],
                trade_dollar_amount=-current_position['dollar_amount'],
                shares_to_trade=abs(current_position['shares']),
                current_shares=current_position['shares'],
                confidence_score=1.0,
                volatility_adjustment=1.0,
                regime_adjustment=1.0,
                sentiment_adjustment=1.0,
                portfolio_limit_applied=False,
                reasoning=f"Closing entire position: {current_position['shares']:.2f} shares",
                risk_warnings=[],
                timestamp=datetime.now()
            )

        except Exception as e:
            self.logger.error(f"Error calculating close size for {symbol}: {e}")
            return self._create_safe_default_result(symbol, 1.0)

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get current portfolio summary for position sizing decisions"""
        try:
            portfolio_value = self._get_portfolio_value()
            all_holdings = self.db_manager.get_holdings(active_only=True)

            total_invested = sum(float(h['total_invested']) for h in all_holdings)
            cash = portfolio_value - total_invested
            position_count = len(all_holdings)

            # Calculate largest position
            largest_position_pct = 0.0
            if all_holdings and portfolio_value > 0:
                largest_value = max(float(h['total_invested']) for h in all_holdings)
                largest_position_pct = (largest_value / portfolio_value) * 100

            # Calculate total allocation
            total_allocation_pct = (total_invested / portfolio_value) * 100 if portfolio_value > 0 else 0

            return {
                'portfolio_value': portfolio_value,
                'cash': cash,
                'total_invested': total_invested,
                'total_allocation_pct': total_allocation_pct,
                'position_count': position_count,
                'largest_position_pct': largest_position_pct,
                'available_for_new_positions_pct': max(0, 80 - total_allocation_pct),
                'positions': [
                    {
                        'symbol': h['symbol'],
                        'shares': float(h['net_quantity']),
                        'cost_basis': float(h['avg_cost_basis']),
                        'invested': float(h['total_invested']),
                        'allocation_pct': (float(h['total_invested']) / portfolio_value) * 100 if portfolio_value > 0 else 0
                    }
                    for h in all_holdings
                ]
            }

        except Exception as e:
            self.logger.error(f"Error getting portfolio summary: {e}")
            return {'error': str(e)}

    # Private helper methods

    def _get_portfolio_value(self) -> float:
        """Get total portfolio value"""
        try:
            if self.alpaca_interface:
                account = self.alpaca_interface.get_account_info()
                if account:
                    return account.portfolio_value

            # Fallback: calculate from database
            holdings = self.db_manager.get_holdings(active_only=True)
            total_invested = sum(float(h['total_invested']) for h in holdings)

            # Estimate cash (this is a simplification)
            estimated_cash = 25000.0  # Default starting cash
            return total_invested + estimated_cash

        except Exception as e:
            self.logger.error(f"Error getting portfolio value: {e}")
            return 50000.0  # Safe default

    def _get_current_position(self, symbol: str) -> Dict[str, float]:
        """Get current position for symbol from database"""
        try:
            holdings = self.db_manager.get_holdings(symbol=symbol, active_only=True)

            if holdings:
                holding = holdings[0]
                shares = float(holding['net_quantity'])
                cost_basis = float(holding['avg_cost_basis'])
                total_invested = float(holding['total_invested'])

                portfolio_value = self._get_portfolio_value()
                size_pct = (total_invested / portfolio_value) if portfolio_value > 0 else 0

                return {
                    'shares': shares,
                    'cost_basis': cost_basis,
                    'dollar_amount': total_invested,
                    'size_pct': size_pct
                }
            else:
                # No position
                return {
                    'shares': 0.0,
                    'cost_basis': 0.0,
                    'dollar_amount': 0.0,
                    'size_pct': 0.0
                }

        except Exception as e:
            self.logger.error(f"Error getting current position for {symbol}: {e}")
            return {'shares': 0.0, 'cost_basis': 0.0, 'dollar_amount': 0.0, 'size_pct': 0.0}

    def _calculate_base_size(self, confidence_score: float) -> float:
        """Calculate base position size from confidence"""
        base_size = self.config['base_position_size_pct']
        confidence_multiplier = 1.0 + (confidence_score - 0.5) * self.config['confidence_scaling_factor']

        # Ensure positive and within reasonable bounds
        confidence_multiplier = max(0.1, min(3.0, confidence_multiplier))

        return base_size * confidence_multiplier

    def _apply_adjustments(self, base_size: float, sentiment_score: float,
                          market_regime: str, volatility: Optional[float]) -> float:
        """Apply various adjustments to base size"""
        adjusted_size = base_size

        # Sentiment adjustment
        sentiment_adj = self._get_sentiment_adjustment(sentiment_score)
        adjusted_size *= sentiment_adj

        # Regime adjustment
        regime_adj = self.config['regime_multipliers'].get(market_regime, 1.0)
        adjusted_size *= regime_adj

        # Volatility adjustment
        if volatility and volatility > self.config['high_volatility_threshold']:
            vol_adj = self.config['volatility_reduction_factor']
            adjusted_size *= vol_adj

        return adjusted_size

    def _get_sentiment_adjustment(self, sentiment_score: float) -> float:
        """Get sentiment-based size adjustment"""
        if sentiment_score >= 0.7:
            return self.config['sentiment_multipliers']['extreme_positive']
        elif sentiment_score >= 0.3:
            return self.config['sentiment_multipliers']['positive']
        elif sentiment_score >= -0.3:
            return self.config['sentiment_multipliers']['neutral']
        elif sentiment_score >= -0.7:
            return self.config['sentiment_multipliers']['negative']
        else:
            return self.config['sentiment_multipliers']['extreme_negative']

    def _get_volatility_adjustment(self, volatility: Optional[float]) -> float:
        """Get volatility adjustment factor"""
        if not volatility:
            return 1.0

        if volatility > self.config['high_volatility_threshold']:
            return self.config['volatility_reduction_factor']
        else:
            return 1.0

    def _apply_portfolio_limits(self, target_size: float, symbol: str,
                               portfolio_value: float) -> Tuple[float, bool]:
        """Apply portfolio-level limits"""
        limit_applied = False

        # Individual position limit (15% max)
        max_individual = self.config['max_position_size_pct']
        if target_size > max_individual:
            target_size = max_individual
            limit_applied = True

        # Minimum position limit
        min_individual = self.config['min_position_size_pct']
        if 0 < target_size < min_individual:
            target_size = min_individual
            limit_applied = True

        # Total portfolio allocation limit (80% max)
        current_total_allocation = self._get_total_allocation_excluding_symbol(symbol)
        max_total = self.config['max_total_allocation_pct']
        available_allocation = max_total - current_total_allocation

        if target_size > available_allocation:
            target_size = max(0, available_allocation)
            limit_applied = True

        return target_size, limit_applied

    def _get_total_allocation_excluding_symbol(self, exclude_symbol: str) -> float:
        """Get total portfolio allocation excluding specified symbol"""
        try:
            holdings = self.db_manager.get_holdings(active_only=True)
            total_invested = sum(
                float(h['total_invested'])
                for h in holdings
                if h['symbol'] != exclude_symbol
            )

            portfolio_value = self._get_portfolio_value()
            return (total_invested / portfolio_value) if portfolio_value > 0 else 0

        except Exception as e:
            self.logger.error(f"Error calculating total allocation: {e}")
            return 0.5  # Conservative estimate

    def _determine_action(self, current_size: float, target_size: float,
                         size_change: float) -> SizeAction:
        """Determine what type of action is needed"""
        threshold = self.config['rebalance_threshold_pct']

        if current_size == 0 and target_size > 0:
            return SizeAction.OPEN_POSITION
        elif current_size > 0 and target_size == 0:
            return SizeAction.CLOSE_POSITION
        elif abs(size_change) < threshold:
            return SizeAction.OPEN_POSITION  # No action needed, but return something
        elif size_change > 0:
            return SizeAction.ADD_TO_POSITION
        else:
            return SizeAction.REDUCE_POSITION

    def _calculate_shares_to_trade(self, dollar_amount: float, price: float) -> float:
        """Calculate number of shares to trade"""
        if price <= 0:
            return 0.0
        return dollar_amount / price

    def _generate_reasoning_and_warnings(self, symbol: str, base_size: float,
                                       final_size: float, confidence: float,
                                       sentiment: float, regime: str,
                                       limit_applied: bool) -> Tuple[str, List[str]]:
        """Generate human-readable reasoning and risk warnings"""

        # Reasoning
        reasoning_parts = [
            f"Base size: {base_size:.1%} (confidence: {confidence:.1%})",
            f"Final size: {final_size:.1%}",
            f"Sentiment: {sentiment:+.2f}",
            f"Regime: {regime}"
        ]

        if limit_applied:
            reasoning_parts.append("Portfolio limits applied")

        reasoning = " | ".join(reasoning_parts)

        # Warnings
        warnings = []

        if final_size >= 0.12:  # 12%+ position
            warnings.append(f"Large position ({final_size:.1%})")

        if final_size >= 0.15:  # At maximum
            warnings.append(f"Maximum position size reached")

        if abs(sentiment) > 0.8:
            warnings.append(f"Extreme sentiment ({sentiment:+.2f})")

        if regime in ['HIGH_VOLATILITY', 'BEAR_MARKET']:
            warnings.append(f"Challenging market: {regime}")

        return reasoning, warnings

    def _create_safe_default_result(self, symbol: str, confidence: float) -> PositionSizeResult:
        """Create safe default result when calculation fails"""
        return PositionSizeResult(
            symbol=symbol,
            action=SizeAction.OPEN_POSITION,
            target_size_pct=0.0,
            current_size_pct=0.0,
            size_change_pct=0.0,
            target_dollar_amount=0.0,
            current_dollar_amount=0.0,
            trade_dollar_amount=0.0,
            shares_to_trade=0.0,
            current_shares=0.0,
            confidence_score=confidence,
            volatility_adjustment=1.0,
            regime_adjustment=1.0,
            sentiment_adjustment=1.0,
            portfolio_limit_applied=False,
            reasoning="Error in calculation - safe default",
            risk_warnings=["Calculation error occurred"],
            timestamp=datetime.now()
        )


# Utility functions for decision engine integration

def calculate_buy_size(symbol: str, confidence: float, sentiment: float = 0.0,
                      regime: str = "UNKNOWN", position_sizer: Optional[LeanPositionSizer] = None) -> PositionSizeResult:
    """Utility function to calculate buy size"""
    if not position_sizer:
        position_sizer = LeanPositionSizer()

    return position_sizer.calculate_position_size(
        symbol=symbol,
        confidence_score=confidence,
        sentiment_score=sentiment,
        market_regime=regime
    )

def calculate_sell_size(symbol: str, position_sizer: Optional[LeanPositionSizer] = None) -> PositionSizeResult:
    """Utility function to calculate sell (close) size"""
    if not position_sizer:
        position_sizer = LeanPositionSizer()

    return position_sizer.calculate_close_size(symbol)

def should_rebalance_position(symbol: str, current_confidence: float,
                            position_sizer: Optional[LeanPositionSizer] = None) -> bool:
    """Check if position should be rebalanced based on new confidence"""
    if not position_sizer:
        position_sizer = LeanPositionSizer()

    result = position_sizer.calculate_position_size(
        symbol=symbol,
        confidence_score=current_confidence
    )

    return result.should_trade
