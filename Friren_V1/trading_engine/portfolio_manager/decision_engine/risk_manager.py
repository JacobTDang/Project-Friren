"""
Solid Risk Manager - Core Safety Validation

Fixed import structure and implementation approach.

Core Responsibilities:
1. Position size limits (15% max per symbol)
2. Portfolio allocation limits (80% max total)
3. Account safety (buying power, margin)
4. Market condition checks (VIX, volatility)
5. One position per symbol rule enforcement
6. Emergency controls
"""

from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging
import sys
import os

# Fixed imports with correct path resolution
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# First, define the SizeCalculation class locally to avoid circular import issues
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

# Import with robust fallback strategy that handles constructor arguments properly
try:
    # Primary import path - assuming correct structure
    from ..tools.position_sizer import PurePositionSizer
    from ..tools.db_manager import TradingDBManager
    from ..tools.alpaca_interface import SimpleAlpacaInterface
    from .conflict_resolver import ResolvedDecision
    # Import enhanced feature engineer for sophisticated risk assessment
    from ...news.recommendations.enhanced_feature_engineer import EnhancedFeatureEngineer
    IMPORTS_SUCCESSFUL = True
except ImportError:
    try:
        # Alternative path - in case we're in different directory structure
        from portfolio_manager.tools.position_sizer import PurePositionSizer
        from portfolio_manager.tools.db_manager import TradingDBManager
        from portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface
        from decision_engine.conflict_resolver import ResolvedDecision
        IMPORTS_SUCCESSFUL = True
    except ImportError:
        try:
            # Try absolute imports with project path
            import sys
            import os

            # Add multiple possible paths
            possible_paths = [
                os.path.join(project_root, 'trading_engine', 'portfolio_manager', 'tools'),
                os.path.join(project_root, 'Friren_V1', 'trading_engine', 'portfolio_manager', 'tools'),
                os.path.dirname(os.path.dirname(__file__))  # parent directory
            ]

            for path in possible_paths:
                if path not in sys.path:
                    sys.path.append(path)

            # Try imports with extended path
            from position_sizer import PurePositionSizer
            from db_manager import TradingDBManager
            from alpaca_interface import SimpleAlpacaInterface

            # Define ResolvedDecision locally if not found
            @dataclass
            class ResolvedDecision:
                symbol: str = "TEST"
                final_direction: float = 0.5
                final_confidence: float = 0.5
                resolution_method: str = "test"

            IMPORTS_SUCCESSFUL = True

        except ImportError:
            # NO FALLBACK - require proper imports for production
            IMPORTS_SUCCESSFUL = False
            raise ImportError("PRODUCTION: Required trading components not available. "
                            "PurePositionSizer, TradingDBManager, and SimpleAlpacaInterface must be properly imported. "
                            "No mock implementations allowed.")


class RiskLevel(Enum):
    """Risk assessment levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationResult(Enum):
    """Risk validation outcomes"""
    APPROVED = "approved"
    DENIED = "denied"
    MODIFIED = "modified"
    EMERGENCY_HALT = "emergency_halt"


@dataclass
class RiskValidationResult:
    """Result of risk validation"""
    validation_result: ValidationResult
    symbol: str
    original_decision: ResolvedDecision
    approved_size_pct: Optional[float] = None
    trade_amount: Optional[float] = None

    # Risk assessment
    risk_level: RiskLevel = RiskLevel.LOW
    risk_score: float = 0.0

    # Validation details
    reason: str = ""
    warnings: List[str] = None
    failed_checks: List[str] = None

    # Execution details (if approved)
    size_calculation: Optional[SizeCalculation] = None

    timestamp: datetime = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.failed_checks is None:
            self.failed_checks = []
        if self.timestamp is None:
            self.timestamp = datetime.now()

    @property
    def is_approved(self) -> bool:
        return self.validation_result in [ValidationResult.APPROVED, ValidationResult.MODIFIED]

    @property
    def should_execute(self) -> bool:
        return self.is_approved and self.size_calculation and self.size_calculation.needs_trade


class SolidRiskManager:
    """
    Solid Risk Manager - Core Safety Without Overengineering

    This is the architectural approach I'm taking as your mentor:

    **Design Philosophy:**
    - Final veto authority over all decisions (fail-safe by default)
    - Enforce core safety rules with clear boundaries
    - Graceful degradation with fallback strategies
    - Performance optimized for t3.micro constraints

    **Key Implementation Decisions:**

    1. **Import Strategy**: Uses robust fallback imports to handle different
        deployment environments. This prevents the system from failing if
        dependencies aren't available during development.

    2. **Risk Scoring**: Simple additive model (0-100 scale) rather than
        complex ML. This keeps it fast and interpretable.

    3. **Position Limits**: Hard-coded safety limits with business logic
        that's easy to understand and modify.

    4. **Error Handling**: Always defaults to denial when uncertain -
        better to miss a trade than risk the portfolio.

    **Integration Points:**
    - Works with existing PurePositionSizer for calculations
    - Uses TradingDBManager for position data
    - Provides clear validation results for decision engine
    """

    def __init__(self,
                 position_sizer: Optional[PurePositionSizer] = None,
                 db_manager: Optional[TradingDBManager] = None,
                 alpaca_interface: Optional[SimpleAlpacaInterface] = None):

        self.logger = logging.getLogger("risk_manager")

        # Tool integrations - using dependency injection pattern
        self.position_sizer = position_sizer or PurePositionSizer()
        self.db_manager = db_manager or TradingDBManager("risk_manager")
        self.alpaca_interface = alpaca_interface

        # Core risk limits - configurable but with sensible defaults
        self.limits = {
            # Position limits (as percentages)
            'max_position_size_pct': 0.15,      # 15% max per symbol
            'min_position_size_pct': 0.005,     # 0.5% minimum (below this = close)
            'max_portfolio_allocation_pct': 0.80, # 80% max total allocation

            # Account safety (dollar amounts)
            'min_buying_power_buffer': 1000.0,   # Keep $1000 cash buffer
            'max_single_trade_amount': 10000.0,  # $10k max single trade

            # Market conditions (volatility indicators)
            'vix_halt_threshold': 45.0,          # Halt trading if VIX > 45
            'volatility_warning_threshold': 0.40, # 40% volatility warning

            # Risk scoring thresholds
            'high_risk_threshold': 70.0,         # 70+ = high risk
            'critical_risk_threshold': 85.0,     # 85+ = critical risk

            # Operational controls
            'max_daily_trades': 50,              # Max 50 trades per day
            'position_loss_alert_threshold': 0.15, # 15% loss alert
        }

        # State tracking for daily limits and emergency controls
        self.daily_trade_count = 0
        self.last_reset_date = datetime.now().date()
        self.emergency_halt_active = False
        self.halt_reason = ""

        # Performance tracking for system optimization
        self.validation_stats = {
            'total_validations': 0,
            'approved_count': 0,
            'denied_count': 0,
            'modified_count': 0,
            'avg_processing_time_ms': 0.0
        }

        # Enhanced performance monitoring for feature analysis
        self.enhanced_performance_stats = {
            'enhanced_feature_calls': 0,
            'enhanced_feature_successes': 0,
            'enhanced_feature_failures': 0,
            'enhanced_sentiment_calls': 0,
            'enhanced_sentiment_successes': 0,
            'avg_enhanced_feature_time_ms': 0.0,
            'avg_enhanced_sentiment_time_ms': 0.0,
            'feature_risk_breakdown': {
                'sentiment_quality_avg': 0.0,
                'market_context_avg': 0.0,
                'data_quality_avg': 0.0
            },
            'performance_alerts': [],
            'last_performance_check': datetime.now()
        }

        # Enhanced feature engineer for sophisticated risk assessment
        try:
            self.enhanced_feature_engineer = EnhancedFeatureEngineer()
            self.logger.info("Enhanced feature engineer integrated for sophisticated risk assessment")
        except Exception as e:
            self.logger.warning(f"Could not initialize enhanced feature engineer: {e}")
            self.enhanced_feature_engineer = None

        # Configuration manager for dynamic parameter adjustment
        try:
            from .risk_config_manager import create_risk_config_manager
            self.config_manager = create_risk_config_manager()
            self.logger.info("Risk configuration manager initialized with dynamic parameter support")
        except Exception as e:
            self.logger.warning(f"Could not initialize risk configuration manager: {e}")
            self.config_manager = None

        self.logger.info("SolidRiskManager initialized with core safety rules and dynamic configuration")

    def validate_decision(self, resolved_decision: ResolvedDecision,
                          market_vix: Optional[float] = None) -> RiskValidationResult:
        """
        Main validation method - final veto authority

        **Flow Explanation:**
        This is where we implement the 'fail-safe' principle. Each validation
        step can return early with a denial, but approval requires passing
        ALL checks. This prevents edge cases from slipping through.

        **Performance Considerations:**
        - Fast-fail checks first (emergency halt, daily limits)
        - Expensive calculations only if needed (position sizing)
        - Caching where possible (portfolio summary)

        Args:
            resolved_decision: Decision from conflict resolver
            market_vix: Current VIX level (optional)

        Returns:
            RiskValidationResult with approval/denial and details
        """
        start_time = datetime.now()

        try:
            self.validation_stats['total_validations'] += 1

            # Step 0: Update configuration based on market conditions (if config manager available)
            if self.config_manager and market_vix is not None:
                config_changed = self.config_manager.update_market_conditions(vix=market_vix)
                if config_changed:
                    # Update local limits from new configuration
                    self._update_limits_from_config()

            # Step 1: Reset daily counter if new day
            self._reset_daily_counters()

            # Step 2: Emergency halt check (fail fast)
            if self.emergency_halt_active:
                return self._create_denial_result(
                    resolved_decision,
                    f"Emergency halt active: {self.halt_reason}",
                    ValidationResult.EMERGENCY_HALT
                )

            # Step 3: Market condition checks (fail fast)
            market_check = self._validate_market_conditions(market_vix)
            if not market_check[0]:
                return self._create_denial_result(resolved_decision, market_check[1])

            # Step 4: Daily trade limit check
            if self.daily_trade_count >= self.limits['max_daily_trades']:
                return self._create_denial_result(
                    resolved_decision,
                    f"Daily trade limit exceeded ({self.limits['max_daily_trades']})"
                )

            # Step 5: Calculate target position size based on confidence/direction
            target_size_pct = self._calculate_target_position_size(resolved_decision)

            # Step 6: Position size validation (with adjustment capability)
            size_check = self._validate_position_size(resolved_decision.symbol, target_size_pct)
            if not size_check[0]:
                return self._create_denial_result(resolved_decision, size_check[1])

            # Apply position size adjustment if suggested
            if len(size_check) > 2 and size_check[2] is not None:
                original_size = target_size_pct
                target_size_pct = size_check[2]
                self.logger.info(f"RISK MANAGER: Adjusted position size for {resolved_decision.symbol}: {target_size_pct:.1%} - {size_check[1]}")

                # VISIBLE OUTPUT: Show position size adjustment
                try:
                    # Import the colored output function used in decision engine
                    import sys, os
                    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
                    sys.path.append(project_root)
                    from main_terminal_bridge import send_colored_business_output
                    send_colored_business_output("risk",
                        f"Risk Manager: ADJUSTED {resolved_decision.symbol} position size {original_size:.1%} → {target_size_pct:.1%}",
                        "risk")
                except Exception:
                    # Fallback to simple print
                    print(f"[RISK] Risk Manager: ADJUSTED {resolved_decision.symbol} position size {original_size:.1%} → {target_size_pct:.1%}")

            # Step 7: Portfolio allocation validation (with modification capability)
            portfolio_check = self._validate_portfolio_allocation(resolved_decision.symbol, target_size_pct)
            if not portfolio_check[0]:
                # Try to modify size instead of denying outright
                modified_size = portfolio_check[2] if len(portfolio_check) > 2 else target_size_pct * 0.5
                if modified_size is not None and modified_size >= self.limits['min_position_size_pct']:
                    target_size_pct = modified_size
                    self.logger.info(f"Modified position size for {resolved_decision.symbol}: {target_size_pct:.1%}")
                else:
                    return self._create_denial_result(resolved_decision, portfolio_check[1])

            # Step 8: Calculate actual trade requirements
            size_calc = self.position_sizer.size_up(resolved_decision.symbol, target_size_pct)

            # Step 9: Account safety validation
            account_check = self._validate_account_safety(size_calc)
            if not account_check[0]:
                return self._create_denial_result(resolved_decision, account_check[1])

            # Step 10: One position per symbol rule
            position_rule_check = self._validate_position_rule(resolved_decision, size_calc)
            if not position_rule_check[0]:
                return self._create_denial_result(resolved_decision, position_rule_check[1])

            # Step 11: Calculate comprehensive risk score
            risk_score, risk_level, warnings = self._calculate_risk_assessment(
                resolved_decision, size_calc
            )

            # Step 12: Final approval decision based on risk level
            if risk_level == RiskLevel.CRITICAL:
                return self._create_denial_result(
                    resolved_decision,
                    f"Critical risk level ({risk_score:.0f}/100)"
                )

            # APPROVED! Update counters and return successful result
            self.daily_trade_count += 1
            self.validation_stats['approved_count'] += 1

            # Update processing time tracking
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_processing_time(processing_time)

            # Update configuration manager with performance metrics
            if self.config_manager:
                success_rate = self.validation_stats['approved_count'] / max(1, self.validation_stats['total_validations'])
                self.config_manager.update_performance_metrics(
                    success_rate=success_rate,
                    avg_processing_time=processing_time,
                    risk_score_accuracy=None  # Could be calculated from historical data
                )

            result = RiskValidationResult(
                validation_result=ValidationResult.APPROVED,
                symbol=resolved_decision.symbol,
                original_decision=resolved_decision,
                approved_size_pct=target_size_pct,
                trade_amount=size_calc.trade_dollar_amount,
                risk_level=risk_level,
                risk_score=risk_score,
                reason=f"Approved with {risk_level.value} risk",
                warnings=warnings,
                size_calculation=size_calc
            )

            self.logger.info(f"APPROVED: {resolved_decision.symbol} at {target_size_pct:.1%} "
                             f"(risk: {risk_level.value}, score: {risk_score:.0f})")

            # BUSINESS LOGIC OUTPUT: Required format from CLAUDE.md
            action = "increase" if size_calc.is_buy else "decrease"
            try:
                from terminal_color_system import send_colored_business_output
                send_colored_business_output("risk_manager", f"Risk Manager: Position size {action} for {resolved_decision.symbol} - risk: {risk_level.value}", "risk_manager")
            except ImportError:
                print(f"[RISK MANAGER] Risk Manager: Position size {action} for {resolved_decision.symbol} - risk: {risk_level.value}")

            # ADDITIONAL: Detailed risk check approval for debugging
            try:
                from terminal_color_system import print_risk_check
                shares_count = int(abs(size_calc.shares_to_trade)) if size_calc.shares_to_trade else 0
                risk_reason = warnings[0] if warnings else f"{risk_level.value.lower().replace('_', ' ')} risk level"
                print_risk_check(f"{resolved_decision.symbol}: PASSED | decision: {action.upper()} {shares_count} shares | risk_score: {risk_score/100:.2f} | reason: '{risk_reason}'")
            except ImportError:
                action = "BUY" if size_calc.is_buy else "SELL"
                shares_count = int(abs(size_calc.shares_to_trade)) if size_calc.shares_to_trade else 0
                risk_reason = warnings[0] if warnings else f"{risk_level.value.lower().replace('_', ' ')} risk level"
                print(f"[RISK CHECK] {resolved_decision.symbol}: PASSED | decision: {action} {shares_count} shares | risk_score: {risk_score/100:.2f} | reason: '{risk_reason}'")

            return result

        except Exception as e:
            self.logger.error(f"Error in risk validation: {e}")
            return self._create_denial_result(
                resolved_decision,
                f"Risk validation error: {e}"
            )

    def emergency_halt_trading(self, reason: str):
        """
        Activate emergency trading halt

        **When to use:**
        - Extreme market conditions (VIX > 45, flash crash)
        - System errors affecting position tracking
        - Account safety issues (margin calls, etc.)
        """
        self.emergency_halt_active = True
        self.halt_reason = reason
        self.logger.warning(f" EMERGENCY HALT ACTIVATED: {reason}")

    def resume_trading(self):
        """Resume trading after emergency halt"""
        self.emergency_halt_active = False
        self.halt_reason = ""
        self.logger.info(" Trading resumed after emergency halt")

    def get_risk_summary(self) -> Dict[str, Any]:
        """
        Get current risk status summary

        **Usage:**
        This is useful for monitoring dashboards and health checks.
        Provides snapshot of current risk state without expensive calculations.
        """
        try:
            portfolio_summary = self.position_sizer.get_portfolio_summary()

            return {
                'emergency_halt_active': self.emergency_halt_active,
                'halt_reason': self.halt_reason,
                'daily_trade_count': self.daily_trade_count,
                'max_daily_trades': self.limits['max_daily_trades'],
                'portfolio_allocation_pct': portfolio_summary.get('allocation_pct', 0),
                'max_allocation_pct': self.limits['max_portfolio_allocation_pct'] * 100,
                'position_count': portfolio_summary.get('position_count', 0),
                'validation_stats': self.validation_stats.copy(),
                'risk_limits': self.limits.copy(),
                'last_reset_date': self.last_reset_date.isoformat()
            }

        except Exception as e:
            self.logger.error(f"Error getting risk summary: {e}")
            return {'error': str(e)}

    # Private validation methods - each handles one specific risk aspect

    def _validate_market_conditions(self, vix: Optional[float]) -> Tuple[bool, str]:
        """
        Validate market conditions for trading

        **Why this matters:**
        During extreme volatility (VIX > 45), algorithms can behave unpredictably.
        Better to halt trading and preserve capital.
        """
        if vix and vix > self.limits['vix_halt_threshold']:
            return False, f"VIX too high ({vix:.1f} > {self.limits['vix_halt_threshold']})"
        return True, "Market conditions acceptable"

    def _validate_position_size(self, symbol: str, target_size_pct: float) -> Tuple[bool, str, Optional[float]]:
        """
        Validate individual position size limits and suggest adjustments

        **Trade-offs:**
        - 15% max position size: Prevents over-concentration risk
        - 0.5% minimum: Avoids tiny positions with high transaction costs

        **NEW APPROACH:** Adjust position size instead of rejecting outright
        """
        if target_size_pct > self.limits['max_position_size_pct']:
            # Adjust to maximum allowed size instead of rejecting
            adjusted_size = self.limits['max_position_size_pct']
            return True, f"Position size adjusted from {target_size_pct:.1%} to max allowed {adjusted_size:.1%}", adjusted_size

        if target_size_pct < self.limits['min_position_size_pct']:
            # Check if we can adjust to minimum viable size
            adjusted_size = self.limits['min_position_size_pct']
            return True, f"Position size adjusted from {target_size_pct:.1%} to minimum viable {adjusted_size:.1%}", adjusted_size

        return True, "Position size within limits", None

    def _validate_portfolio_allocation(self, symbol: str, target_size_pct: float) -> Tuple[bool, str, Optional[float]]:
        """
        Validate portfolio allocation limits

        **Key insight:**
        This is where we can be smart about modification vs denial.
        If the target would exceed limits, we calculate the maximum allowable size.
        """
        try:
            portfolio_summary = self.position_sizer.get_portfolio_summary()
            current_allocation_pct = portfolio_summary.get('allocation_pct', 0) / 100

            # Calculate current allocation excluding this symbol
            current_position_pct = self.position_sizer.get_current_size(symbol)
            other_allocation = current_allocation_pct - current_position_pct

            # Calculate new total allocation
            new_total_allocation = other_allocation + target_size_pct
            max_allocation = self.limits['max_portfolio_allocation_pct']

            if new_total_allocation > max_allocation:
                # Calculate max possible size for this symbol
                available_allocation = max_allocation - other_allocation
                if available_allocation > 0:
                    return False, f"Would exceed portfolio limit ({new_total_allocation:.1%} > {max_allocation:.1%})", available_allocation
                else:
                    return False, f"No allocation available (current: {current_allocation_pct:.1%})", None

            return True, "Portfolio allocation within limits", None

        except Exception as e:
            self.logger.error(f"Error validating portfolio allocation: {e}")
            return False, f"Portfolio validation error: {e}", None

    def _validate_account_safety(self, size_calc: SizeCalculation) -> Tuple[bool, str]:
        """
        Validate account safety (buying power, trade size)

        **Conservative approach:**
        We maintain cash buffers and check buying power to prevent margin calls.
        """
        if not size_calc.needs_trade:
            return True, "No trade needed"

        # Check single trade amount limit
        if abs(size_calc.trade_dollar_amount) > self.limits['max_single_trade_amount']:
            return False, f"Trade amount ${abs(size_calc.trade_dollar_amount):,.0f} exceeds limit ${self.limits['max_single_trade_amount']:,.0f}"

        # Check buying power (for buy orders)
        if size_calc.is_buy:
            if self.alpaca_interface:
                account = self.alpaca_interface.get_account_info()
                if account:
                    required_amount = size_calc.trade_dollar_amount + self.limits['min_buying_power_buffer']
                    if account.buying_power < required_amount:
                        return False, f"Insufficient buying power: need ${required_amount:,.0f}, have ${account.buying_power:,.0f}"

        return True, "Account safety checks passed"

    def _validate_position_rule(self, decision: ResolvedDecision, size_calc: SizeCalculation) -> Tuple[bool, str]:
        """
        Validate one position per symbol rule

        **Business logic:**
        This prevents having both long and short positions in the same symbol,
        which would create unnecessary complexity and risk.
        """
        current_shares = size_calc.current_shares
        target_direction = decision.final_direction

        # Check if this would violate the one position per symbol rule
        if current_shares > 0 and target_direction < -0.3 and size_calc.is_buy:
            return False, "Cannot buy when holding long position and signal is bearish"

        if current_shares < 0 and target_direction > 0.3 and not size_calc.is_buy:
            return False, "Cannot sell when holding short position and signal is bullish"

        return True, "Position rule compliance verified"

    def _calculate_target_position_size(self, decision: ResolvedDecision) -> float:
        """
        Calculate target position size with enhanced position-aware logic and feature engineering

        **Enhanced scaling logic:**
        - Initial positions: 3-12% based on confidence and sentiment
        - Position additions: 1-3% when sentiment >= 0.7
        - Sentiment modifiers: 0.8x to 1.2x based on sentiment score
        - Feature-based risk adjustments using enhanced feature engineering
        - Hard cap at 15% per symbol
        """
        # Get current position
        current_position_pct = 0.0
        if self.position_sizer:
            current_position_pct = self.position_sizer.get_current_size(decision.symbol)

        # Get sentiment score from decision or enhanced features
        sentiment_score = getattr(decision, 'sentiment_score', 0.0) or 0.0

        # Use enhanced feature engineer for more sophisticated sentiment analysis with performance monitoring
        enhanced_sentiment = self._get_enhanced_sentiment_score_with_monitoring(decision)
        if enhanced_sentiment is not None:
            sentiment_score = enhanced_sentiment
            self.logger.debug(f"Using enhanced sentiment score for {decision.symbol}: {sentiment_score:.3f}")

        # If we already have a position and this is a BUY decision
        if current_position_pct > 0 and decision.final_direction > 0:
            # Check if sentiment allows position addition
            if sentiment_score >= 0.7 and current_position_pct < 0.15:
                # Calculate additional position size (1-3% based on sentiment)
                sentiment_excess = sentiment_score - 0.7
                max_excess = 0.3  # 0.7 to 1.0 range
                additional = 0.01 + (sentiment_excess / max_excess) * 0.02  # 1-3% range

                # Don't exceed 15% total or 3% addition
                max_additional = min(0.03, 0.15 - current_position_pct)
                additional = min(additional, max_additional)

                target_pct = current_position_pct + additional
                self.logger.info(f"Position addition sizing for {decision.symbol}: "
                               f"current {current_position_pct:.1%} + {additional:.1%} = {target_pct:.1%}")
                return target_pct
            else:
                # Don't add to position - maintain current
                return current_position_pct

        # For new positions or sell decisions, use enhanced logic
        if decision.final_direction <= 0:  # SELL or HOLD
            return 0.0  # Close position

        # Calculate initial position size with sentiment-based sizing
        base_size = min(0.03 + (decision.final_confidence * 0.09), 0.12)  # 3-12% range

        # Sentiment modifier (0.8x to 1.2x)
        sentiment_modifier = 1.0 + (sentiment_score * 0.2)
        sentiment_modifier = max(0.8, min(1.2, sentiment_modifier))

        # Apply sentiment modifier
        target_size = base_size * sentiment_modifier

        # Direction strength adjustment
        direction_multiplier = abs(decision.final_direction)
        adjusted_size = target_size * direction_multiplier

        # Safety limits: 3% minimum, 15% maximum
        final_size = max(0.03, min(0.15, adjusted_size))

        self.logger.debug(f"Initial position sizing for {decision.symbol}: "
                        f"base {base_size:.1%} * sentiment {sentiment_modifier:.2f} * "
                        f"strength {direction_multiplier:.2f} = {final_size:.1%}")

        return final_size

    def _calculate_risk_assessment(self, decision: ResolvedDecision,
                                   size_calc: SizeCalculation) -> Tuple[float, RiskLevel, List[str]]:
        """
        Calculate overall risk score and level with enhanced feature-based analysis

        **Enhanced risk factors (0-100 scale):**
        - Position size risk (0-25 points)
        - Confidence risk (0-20 points)
        - Trade size risk (0-15 points)
        - Portfolio concentration risk (0-20 points)
        - Enhanced feature risk (0-20 points) - NEW: sentiment quality, market context, etc.

        **Enhancement approach:**
        Combines simple additive model with sophisticated feature analysis from
        EnhancedFeatureEngineer for better risk detection while maintaining speed.
        """
        risk_factors = []
        warnings = []

        # Position size risk (0-25 points) - reduced to make room for feature analysis
        size_risk = (size_calc.target_size_pct / self.limits['max_position_size_pct']) * 25
        risk_factors.append(size_risk)

        if size_calc.target_size_pct > 0.10:    # 10%+
            warnings.append(f"Large position size: {size_calc.target_size_pct:.1%}")

        # Confidence risk (0-20 points) - lower confidence = higher risk
        confidence_risk = (1.0 - decision.final_confidence) * 20
        risk_factors.append(confidence_risk)

        if decision.final_confidence < 0.7:
            warnings.append(f"Low confidence: {decision.final_confidence:.1%}")

        # Trade size risk (0-15 points)
        trade_size_risk = min((abs(size_calc.trade_dollar_amount) / self.limits['max_single_trade_amount']) * 15, 15)
        risk_factors.append(trade_size_risk)

        # Enhanced feature-based risk analysis (0-20 points) with performance monitoring
        feature_risk, feature_warnings = self._calculate_enhanced_feature_risk_with_monitoring(decision)
        risk_factors.append(feature_risk)
        warnings.extend(feature_warnings)

        # Portfolio concentration risk (0-20 points)
        portfolio_summary = self.position_sizer.get_portfolio_summary()
        allocation_pct = portfolio_summary.get('allocation_pct', 0) / 100
        concentration_risk = (allocation_pct / self.limits['max_portfolio_allocation_pct']) * 20
        risk_factors.append(concentration_risk)

        if allocation_pct > 0.60:    # 60%+
            warnings.append(f"High portfolio allocation: {allocation_pct:.1%}")

        # Calculate total risk score
        total_risk_score = sum(risk_factors)

        # Determine risk level
        if total_risk_score >= self.limits['critical_risk_threshold']:
            risk_level = RiskLevel.CRITICAL
        elif total_risk_score >= self.limits['high_risk_threshold']:
            risk_level = RiskLevel.HIGH
        elif total_risk_score >= 40:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW

        # Log enhanced risk breakdown for debugging
        self.logger.debug(f"Risk breakdown for {decision.symbol}: "
                         f"size={size_risk:.1f}, confidence={confidence_risk:.1f}, "
                         f"trade_size={trade_size_risk:.1f}, features={feature_risk:.1f}, "
                         f"concentration={concentration_risk:.1f}, total={total_risk_score:.1f}")

        return total_risk_score, risk_level, warnings

    def _create_denial_result(self, decision: ResolvedDecision, reason: str,
                              result_type: ValidationResult = ValidationResult.DENIED) -> RiskValidationResult:
        """Create denial result with proper tracking"""
        self.validation_stats['denied_count'] += 1

        self.logger.info(f"DENIED: {decision.symbol} - {reason}")

        # BUSINESS LOGIC OUTPUT: Required format from CLAUDE.md
        try:
            from terminal_color_system import send_colored_business_output
            send_colored_business_output("risk_manager", f"Risk Manager: Position size denied for {decision.symbol} - risk: critical", "risk_manager")
        except ImportError:
            print(f"[RISK MANAGER] Risk Manager: Position size denied for {decision.symbol} - risk: critical")

        # ADDITIONAL: Detailed risk check denial for debugging
        try:
            from terminal_color_system import print_risk_check
            print_risk_check(f"FAILED - {decision.symbol}: {reason}")
        except ImportError:
            print(f"[RISK CHECK] FAILED - {decision.symbol}: {reason}")

        return RiskValidationResult(
            validation_result=result_type,
            symbol=decision.symbol,
            original_decision=decision,
            reason=reason,
            failed_checks=[reason]
        )

    def _reset_daily_counters(self):
        """Reset daily counters if new day"""
        today = datetime.now().date()
        if today > self.last_reset_date:
            self.daily_trade_count = 0
            self.last_reset_date = today

    def _update_processing_time(self, processing_time_ms: float):
        """Update average processing time for performance monitoring"""
        total = self.validation_stats['total_validations']
        old_avg = self.validation_stats['avg_processing_time_ms']
        self.validation_stats['avg_processing_time_ms'] = ((old_avg * (total-1)) + processing_time_ms) / total

    def _update_limits_from_config(self):
        """Update risk limits from current configuration manager settings"""
        if not self.config_manager:
            return

        try:
            config = self.config_manager.get_current_config()

            # Update core risk limits
            self.limits.update({
                'max_position_size_pct': config.max_position_size_pct,
                'min_position_size_pct': config.min_position_size_pct,
                'max_portfolio_allocation_pct': config.max_portfolio_allocation_pct,
                'max_daily_trades': config.max_daily_trades,
                'vix_halt_threshold': config.vix_halt_threshold,
                'volatility_warning_threshold': config.volatility_warning_threshold,
                'high_risk_threshold': config.high_risk_threshold,
                'critical_risk_threshold': config.critical_risk_threshold
            })

            self.logger.debug(f"Risk limits updated from configuration: "
                            f"max_position={config.max_position_size_pct:.1%}, "
                            f"approval_threshold={config.approval_threshold:.0%}")

        except Exception as e:
            self.logger.error(f"Error updating limits from configuration: {e}")

    def _get_enhanced_sentiment_score(self, decision: ResolvedDecision) -> Optional[float]:
        """
        Get enhanced sentiment score using sophisticated feature analysis with comprehensive error handling

        Returns refined sentiment score based on:
        - Sentiment quality and consistency
        - Market context and timing
        - News quality and source diversity
        - Financial keyword analysis

        **Resilience Features:**
        - Graceful degradation if enhanced feature engineer unavailable
        - Fallback to basic sentiment if enhanced calculation fails
        - Conservative defaults for missing data
        - Exception isolation to prevent risk manager failure
        """
        # Early return if enhanced feature engineer not available
        if not self.enhanced_feature_engineer:
            self.logger.debug(f"Enhanced feature engineer not available for {decision.symbol} - using basic sentiment")
            return None

        try:
            # Method 1: Use enhanced sentiment data if available
            if hasattr(decision, 'enhanced_sentiment_data') and decision.enhanced_sentiment_data:
                try:
                    sentiment_data = decision.enhanced_sentiment_data

                    # Validate required fields with safe defaults
                    raw_sentiment = sentiment_data.get('raw_sentiment', 0.0)
                    confidence = max(0.1, min(1.0, sentiment_data.get('confidence', 0.5)))  # Clamp confidence
                    market_context = sentiment_data.get('market_context', {})
                    quality_score = max(0.1, min(1.0, sentiment_data.get('quality_score', 0.5)))  # Clamp quality

                    # Use enhanced feature engineer calculation with error isolation
                    enhanced_score = self._safe_enhanced_sentiment_calculation(
                        raw_sentiment, confidence, market_context, quality_score, decision.symbol
                    )

                    if enhanced_score is not None:
                        self.logger.debug(f"Enhanced sentiment calculated for {decision.symbol}: {enhanced_score:.3f}")
                        return enhanced_score

                except Exception as e:
                    self.logger.warning(f"Enhanced sentiment data processing failed for {decision.symbol}: {e}")
                    # Continue to fallback methods

            # Method 2: Construct enhanced sentiment from basic decision attributes
            basic_sentiment = getattr(decision, 'sentiment_score', 0.0) or 0.0
            basic_confidence = getattr(decision, 'final_confidence', 0.5) or 0.5

            if basic_sentiment != 0.0 or basic_confidence > 0.6:
                try:
                    # Create minimal market context from available data
                    market_context = {
                        'volatility': getattr(decision, 'volatility_score', 0.3),
                        'volume_factor': 1.0  # Neutral assumption
                    }

                    enhanced_score = self._safe_enhanced_sentiment_calculation(
                        basic_sentiment, basic_confidence, market_context, 0.5, decision.symbol
                    )

                    if enhanced_score is not None:
                        self.logger.debug(f"Fallback enhanced sentiment for {decision.symbol}: {enhanced_score:.3f}")
                        return enhanced_score

                except Exception as e:
                    self.logger.warning(f"Fallback enhanced sentiment failed for {decision.symbol}: {e}")

            # Method 3: If all enhanced methods fail, return None (use basic sentiment)
            self.logger.debug(f"No enhanced sentiment available for {decision.symbol} - using basic sentiment")
            return None

        except Exception as e:
            # Critical error isolation - never let enhanced sentiment failure break risk assessment
            self.logger.error(f"Critical error in enhanced sentiment calculation for {decision.symbol}: {e}")
            self.logger.error("Enhanced sentiment disabled for this decision - using basic sentiment")
            return None

    def _safe_enhanced_sentiment_calculation(self, raw_sentiment: float, confidence: float,
                                           market_context: Dict[str, Any], quality_score: float,
                                           symbol: str) -> Optional[float]:
        """
        Safely calculate enhanced sentiment with error isolation and validation

        Args:
            raw_sentiment: Base sentiment score
            confidence: Confidence level
            market_context: Market context data
            quality_score: Quality score
            symbol: Symbol for logging

        Returns:
            Enhanced sentiment score or None if calculation fails
        """
        try:
            # Validate inputs
            if not (-1.0 <= raw_sentiment <= 1.0):
                self.logger.warning(f"Invalid raw sentiment {raw_sentiment} for {symbol}, clamping to range")
                raw_sentiment = max(-1.0, min(1.0, raw_sentiment))

            if not (0.0 <= confidence <= 1.0):
                self.logger.warning(f"Invalid confidence {confidence} for {symbol}, clamping to range")
                confidence = max(0.0, min(1.0, confidence))

            # Use enhanced feature engineer with minimal article text for keyword analysis
            article_text = f"Financial analysis for {symbol}"  # Minimal text for API compatibility

            enhanced_score = self.enhanced_feature_engineer.calculate_enhanced_sentiment_score(
                raw_sentiment=raw_sentiment,
                confidence=confidence,
                article_text=article_text,
                market_context=market_context
            )

            # Validate output
            if enhanced_score is not None and (-1.0 <= enhanced_score <= 1.0):
                return enhanced_score
            else:
                self.logger.warning(f"Enhanced sentiment score {enhanced_score} out of range for {symbol}")
                return None

        except Exception as e:
            self.logger.debug(f"Safe enhanced sentiment calculation failed for {symbol}: {e}")
            return None

    def _calculate_enhanced_feature_risk(self, decision: ResolvedDecision) -> Tuple[float, List[str]]:
        """
        Calculate risk based on enhanced feature analysis with comprehensive error handling

        Analyzes:
        - Sentiment quality and consistency
        - Market timing and context
        - News quality and reliability
        - Financial keyword sentiment strength
        - Data freshness and completeness

        **Resilience Features:**
        - Graceful degradation when enhanced features unavailable
        - Individual risk component isolation to prevent total failure
        - Conservative fallbacks for each risk component
        - Detailed error tracking and recovery

        Returns:
            Tuple of (risk_score_0_to_20, warnings_list)
        """
        feature_warnings = []
        feature_risk = 0.0

        # Early fallback if enhanced feature engineer not available
        if not self.enhanced_feature_engineer:
            self.logger.debug(f"Enhanced feature engineer unavailable for {decision.symbol} - using moderate risk")
            return 10.0, ["Enhanced feature analysis unavailable - using moderate risk baseline"]

        try:
            # Sentiment quality risk assessment (0-8 points) with error isolation
            try:
                sentiment_quality_risk = self._assess_sentiment_quality_risk(decision)
                feature_risk += sentiment_quality_risk

                if sentiment_quality_risk > 5:
                    feature_warnings.append("Poor sentiment quality detected")

                self.logger.debug(f"Sentiment quality risk for {decision.symbol}: {sentiment_quality_risk:.1f}/8")

            except Exception as e:
                self.logger.warning(f"Sentiment quality risk assessment failed for {decision.symbol}: {e}")
                fallback_sentiment_risk = 4.0  # Moderate risk fallback
                feature_risk += fallback_sentiment_risk
                feature_warnings.append("Sentiment quality assessment failed - using moderate risk")

            # Market context risk assessment (0-6 points) with error isolation
            try:
                market_context_risk = self._assess_market_context_risk(decision)
                feature_risk += market_context_risk

                if market_context_risk > 4:
                    feature_warnings.append("Unfavorable market context")

                self.logger.debug(f"Market context risk for {decision.symbol}: {market_context_risk:.1f}/6")

            except Exception as e:
                self.logger.warning(f"Market context risk assessment failed for {decision.symbol}: {e}")
                fallback_market_risk = 3.0  # Moderate risk fallback
                feature_risk += fallback_market_risk
                feature_warnings.append("Market context assessment failed - using moderate risk")

            # Data quality risk assessment (0-6 points) with error isolation
            try:
                data_quality_risk = self._assess_data_quality_risk(decision)
                feature_risk += data_quality_risk

                if data_quality_risk > 4:
                    feature_warnings.append("Data quality concerns")

                self.logger.debug(f"Data quality risk for {decision.symbol}: {data_quality_risk:.1f}/6")

            except Exception as e:
                self.logger.warning(f"Data quality risk assessment failed for {decision.symbol}: {e}")
                fallback_data_risk = 3.0  # Moderate risk fallback
                feature_risk += fallback_data_risk
                feature_warnings.append("Data quality assessment failed - using moderate risk")

            # Validate and cap total feature risk
            feature_risk = max(0.0, min(20.0, feature_risk))

            self.logger.debug(f"Total enhanced feature risk for {decision.symbol}: {feature_risk:.1f}/20")

        except Exception as e:
            # Critical error - use conservative fallback for entire feature risk calculation
            self.logger.error(f"Critical error in enhanced feature risk calculation for {decision.symbol}: {e}")
            feature_risk = 15.0  # Conservative high-risk fallback
            feature_warnings = ["Critical feature analysis error - using conservative high-risk assessment"]

            # Log the error for debugging but don't let it break the risk manager
            import traceback
            self.logger.debug(f"Enhanced feature risk calculation traceback: {traceback.format_exc()}")

        return feature_risk, feature_warnings

    def _assess_sentiment_quality_risk(self, decision: ResolvedDecision) -> float:
        """
        Assess risk based on sentiment analysis quality

        Higher risk for:
        - Low sentiment confidence scores
        - Conflicting sentiment signals
        - Weak financial keyword presence
        """
        sentiment_confidence = getattr(decision, 'sentiment_confidence', 0.5)
        sentiment_volatility = getattr(decision, 'sentiment_volatility', 0.3)

        # Low confidence = higher risk (0-4 points)
        confidence_risk = (1.0 - sentiment_confidence) * 4.0

        # High volatility = higher risk (0-4 points)
        volatility_risk = min(sentiment_volatility * 4.0, 4.0)

        return confidence_risk + volatility_risk

    def _assess_market_context_risk(self, decision: ResolvedDecision) -> float:
        """
        Assess risk based on market context and timing

        Higher risk for:
        - Poor market timing (after hours, low volume periods)
        - High market volatility periods
        - Sector correlation issues
        """
        # Market hours factor risk (0-2 points)
        market_hours_factor = getattr(decision, 'market_hours_factor', 0.7)
        timing_risk = (1.0 - market_hours_factor) * 2.0

        # Volatility risk (0-2 points)
        volatility_score = getattr(decision, 'volatility_score', 0.3)
        volatility_risk = min(volatility_score * 2.0, 2.0)

        # Sector correlation risk (0-2 points)
        sector_correlation = getattr(decision, 'sector_correlation', 0.5)
        correlation_risk = (1.0 - sector_correlation) * 2.0

        return timing_risk + volatility_risk + correlation_risk

    def _assess_data_quality_risk(self, decision: ResolvedDecision) -> float:
        """
        Assess risk based on data quality and completeness

        Higher risk for:
        - Stale or incomplete data
        - Low source diversity
        - Poor data freshness
        """
        # Data freshness risk (0-3 points)
        data_freshness = getattr(decision, 'data_freshness', 0.5)
        freshness_risk = (1.0 - data_freshness) * 3.0

        # Source diversity risk (0-3 points)
        source_diversity = getattr(decision, 'source_diversity', 0.5)
        diversity_risk = (1.0 - source_diversity) * 3.0

        return freshness_risk + diversity_risk

    # Performance monitoring wrapper methods

    def _get_enhanced_sentiment_score_with_monitoring(self, decision: ResolvedDecision) -> Optional[float]:
        """
        Performance-monitored wrapper for enhanced sentiment score calculation

        Tracks:
        - Call frequency and success rate
        - Processing time per call
        - Performance alerts for slow operations
        """
        start_time = datetime.now()
        self.enhanced_performance_stats['enhanced_sentiment_calls'] += 1

        try:
            result = self._get_enhanced_sentiment_score(decision)

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_enhanced_sentiment_performance(processing_time, success=True)

            if result is not None:
                self.enhanced_performance_stats['enhanced_sentiment_successes'] += 1

                # Performance alert if processing takes too long
                if processing_time > 100:  # 100ms threshold
                    alert = f"Slow enhanced sentiment calculation for {decision.symbol}: {processing_time:.1f}ms"
                    self._add_performance_alert(alert)
                    self.logger.warning(alert)

            return result

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_enhanced_sentiment_performance(processing_time, success=False)
            self.logger.error(f"Enhanced sentiment monitoring error for {decision.symbol}: {e}")
            return None

    def _calculate_enhanced_feature_risk_with_monitoring(self, decision: ResolvedDecision) -> Tuple[float, List[str]]:
        """
        Performance-monitored wrapper for enhanced feature risk calculation

        Tracks:
        - Feature analysis processing times
        - Success/failure rates for each risk component
        - Performance degradation alerts
        """
        start_time = datetime.now()
        self.enhanced_performance_stats['enhanced_feature_calls'] += 1

        try:
            feature_risk, warnings = self._calculate_enhanced_feature_risk(decision)

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_enhanced_feature_performance(processing_time, success=True)

            self.enhanced_performance_stats['enhanced_feature_successes'] += 1

            # Performance alert if processing takes too long
            if processing_time > 200:  # 200ms threshold for feature analysis
                alert = f"Slow enhanced feature analysis for {decision.symbol}: {processing_time:.1f}ms"
                self._add_performance_alert(alert)
                self.logger.warning(alert)

            # Update feature risk breakdown for monitoring
            self._update_feature_risk_breakdown(feature_risk)

            return feature_risk, warnings

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_enhanced_feature_performance(processing_time, success=False)
            self.enhanced_performance_stats['enhanced_feature_failures'] += 1

            self.logger.error(f"Enhanced feature risk monitoring error for {decision.symbol}: {e}")
            # Return conservative fallback
            return 15.0, [f"Feature analysis monitoring error - using conservative risk"]

    def _update_enhanced_sentiment_performance(self, processing_time_ms: float, success: bool):
        """Update enhanced sentiment performance statistics"""
        try:
            # Update average processing time
            calls = self.enhanced_performance_stats['enhanced_sentiment_calls']
            old_avg = self.enhanced_performance_stats['avg_enhanced_sentiment_time_ms']

            if calls > 0:
                self.enhanced_performance_stats['avg_enhanced_sentiment_time_ms'] = \
                    ((old_avg * (calls - 1)) + processing_time_ms) / calls
            else:
                self.enhanced_performance_stats['avg_enhanced_sentiment_time_ms'] = processing_time_ms

        except Exception as e:
            self.logger.debug(f"Error updating enhanced sentiment performance: {e}")

    def _update_enhanced_feature_performance(self, processing_time_ms: float, success: bool):
        """Update enhanced feature analysis performance statistics"""
        try:
            # Update average processing time
            calls = self.enhanced_performance_stats['enhanced_feature_calls']
            old_avg = self.enhanced_performance_stats['avg_enhanced_feature_time_ms']

            if calls > 0:
                self.enhanced_performance_stats['avg_enhanced_feature_time_ms'] = \
                    ((old_avg * (calls - 1)) + processing_time_ms) / calls
            else:
                self.enhanced_performance_stats['avg_enhanced_feature_time_ms'] = processing_time_ms

        except Exception as e:
            self.logger.debug(f"Error updating enhanced feature performance: {e}")

    def _update_feature_risk_breakdown(self, total_feature_risk: float):
        """Update feature risk breakdown averages for monitoring trends"""
        try:
            # Estimate breakdown (this is approximate since we don't track individual components in production)
            breakdown = self.enhanced_performance_stats['feature_risk_breakdown']

            # Update rolling averages
            calls = self.enhanced_performance_stats['enhanced_feature_calls']
            if calls > 1:
                decay_factor = 0.9  # Give more weight to recent values

                # Rough estimates based on total feature risk
                estimated_sentiment = min(8.0, total_feature_risk * 0.4)  # ~40% of total
                estimated_market = min(6.0, total_feature_risk * 0.3)     # ~30% of total
                estimated_data = min(6.0, total_feature_risk * 0.3)       # ~30% of total

                breakdown['sentiment_quality_avg'] = \
                    breakdown['sentiment_quality_avg'] * decay_factor + estimated_sentiment * (1 - decay_factor)
                breakdown['market_context_avg'] = \
                    breakdown['market_context_avg'] * decay_factor + estimated_market * (1 - decay_factor)
                breakdown['data_quality_avg'] = \
                    breakdown['data_quality_avg'] * decay_factor + estimated_data * (1 - decay_factor)

        except Exception as e:
            self.logger.debug(f"Error updating feature risk breakdown: {e}")

    def _add_performance_alert(self, alert_message: str):
        """Add performance alert with timestamp and cleanup old alerts"""
        try:
            alert = {
                'timestamp': datetime.now(),
                'message': alert_message
            }

            self.enhanced_performance_stats['performance_alerts'].append(alert)

            # Keep only last 10 alerts to prevent memory growth
            if len(self.enhanced_performance_stats['performance_alerts']) > 10:
                self.enhanced_performance_stats['performance_alerts'] = \
                    self.enhanced_performance_stats['performance_alerts'][-10:]

        except Exception as e:
            self.logger.debug(f"Error adding performance alert: {e}")

    def get_enhanced_performance_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive performance summary for enhanced risk assessment

        Returns:
            Dictionary with performance metrics, success rates, and alerts
        """
        try:
            stats = self.enhanced_performance_stats

            # Calculate success rates
            sentiment_success_rate = 0.0
            if stats['enhanced_sentiment_calls'] > 0:
                sentiment_success_rate = stats['enhanced_sentiment_successes'] / stats['enhanced_sentiment_calls']

            feature_success_rate = 0.0
            if stats['enhanced_feature_calls'] > 0:
                feature_success_rate = stats['enhanced_feature_successes'] / stats['enhanced_feature_calls']

            # Recent alerts (last 5)
            recent_alerts = stats['performance_alerts'][-5:] if stats['performance_alerts'] else []

            return {
                'call_statistics': {
                    'enhanced_sentiment_calls': stats['enhanced_sentiment_calls'],
                    'enhanced_sentiment_success_rate': sentiment_success_rate,
                    'enhanced_feature_calls': stats['enhanced_feature_calls'],
                    'enhanced_feature_success_rate': feature_success_rate
                },
                'performance_metrics': {
                    'avg_enhanced_sentiment_time_ms': stats['avg_enhanced_sentiment_time_ms'],
                    'avg_enhanced_feature_time_ms': stats['avg_enhanced_feature_time_ms'],
                    'sentiment_time_ok': stats['avg_enhanced_sentiment_time_ms'] < 100,
                    'feature_time_ok': stats['avg_enhanced_feature_time_ms'] < 200
                },
                'feature_risk_trends': stats['feature_risk_breakdown'].copy(),
                'recent_alerts': [
                    {
                        'timestamp': alert['timestamp'].isoformat(),
                        'message': alert['message']
                    } for alert in recent_alerts
                ],
                'performance_health': {
                    'overall_health': 'good' if sentiment_success_rate > 0.8 and feature_success_rate > 0.8 else 'degraded',
                    'last_check': stats['last_performance_check'].isoformat()
                }
            }

        except Exception as e:
            self.logger.error(f"Error generating enhanced performance summary: {e}")
            return {'error': str(e)}


# Utility functions for integration with decision engine

def validate_trading_decision(decision: ResolvedDecision,
                              risk_manager: Optional[SolidRiskManager] = None,
                              market_vix: Optional[float] = None) -> RiskValidationResult:
    """
    Utility function to validate a trading decision

    **Usage in decision engine:**
    ```python
    from .risk_manager import validate_trading_decision

    validation_result = validate_trading_decision(resolved_decision, risk_manager, vix)
    if validation_result.is_approved:
        execute_trade(validation_result.size_calculation)
    ```
    """
    if not risk_manager:
        risk_manager = SolidRiskManager()

    return risk_manager.validate_decision(decision, market_vix)


def check_emergency_conditions(market_vix: Optional[float] = None,
                               volatility: Optional[float] = None) -> Tuple[bool, str]:
    """
    Check if emergency halt conditions are met

    **Monitoring integration:**
    This can be called by monitoring processes to automatically trigger
    emergency halts when extreme conditions are detected.
    """
    if market_vix and market_vix > 45.0:
        return True, f"Extreme VIX level: {market_vix:.1f}"

    if volatility and volatility > 0.50:    # 50% volatility
        return True, f"Extreme volatility: {volatility:.1%}"

    return False, "No emergency conditions detected"


# Add after the existing SolidRiskManager class, before the utility functions

class EnhancedRiskManager(SolidRiskManager):
    """
    Enhanced Risk Manager with dynamic profiles and Windows compatibility

    Extends SolidRiskManager with:
    - Dynamic risk profiles (conservative/moderate/aggressive) based on market conditions
    - Volatility-adjusted sentiment thresholds
    - Position rebalancing logic with time and allocation triggers
    - Partial exit capabilities for large positions with negative sentiment
    - Enhanced ASCII-only logging for Windows compatibility
    - Market condition-based risk profile switching
    """

    def __init__(self,
                 position_sizer: Optional[PurePositionSizer] = None,
                 db_manager: Optional[TradingDBManager] = None,
                 alpaca_interface: Optional[SimpleAlpacaInterface] = None):

        super().__init__(position_sizer, db_manager, alpaca_interface)

        # Enhanced logging with ASCII-only characters
        self.logger = logging.getLogger("enhanced_risk_manager")

        # Inherit enhanced feature engineer from parent
        if hasattr(super(), 'enhanced_feature_engineer'):
            self.enhanced_feature_engineer = super().enhanced_feature_engineer
        else:
            try:
                self.enhanced_feature_engineer = EnhancedFeatureEngineer()
                self.logger.info("Enhanced feature engineer integrated in EnhancedRiskManager")
            except Exception as e:
                self.logger.warning(f"Could not initialize enhanced feature engineer in EnhancedRiskManager: {e}")
                self.enhanced_feature_engineer = None

        # Dynamic risk profiles based on market conditions
        self.risk_profiles = {
            'conservative': {
                'approval_threshold': 0.75,  # 75% confidence required
                'max_position_pct': 0.08,    # 8% max position size
                'max_portfolio_allocation': 0.60,  # 60% max allocation
                'daily_trade_limit': 10,     # 10 trades max per day
                'vix_threshold': 25.0,       # Conservative if VIX > 25
                'description': 'Conservative risk profile for volatile markets'
            },
            'moderate': {
                'approval_threshold': 0.65,  # 65% confidence required
                'max_position_pct': 0.12,    # 12% max position size
                'max_portfolio_allocation': 0.75,  # 75% max allocation
                'daily_trade_limit': 25,     # 25 trades max per day
                'vix_threshold': 35.0,       # Moderate risk tolerance
                'description': 'Moderate risk profile for stable markets'
            },
            'aggressive': {
                'approval_threshold': 0.55,  # 55% confidence required
                'max_position_pct': 0.18,    # 18% max position size
                'max_portfolio_allocation': 0.85,  # 85% max allocation
                'daily_trade_limit': 50,     # 50 trades max per day
                'vix_threshold': 45.0,       # High risk tolerance
                'description': 'Aggressive risk profile for trending markets'
            }
        }

        # Current active profile (starts as moderate)
        self.current_profile = 'moderate'
        self.profile_change_history = []

        # Enhanced market condition tracking
        self.market_conditions = {
            'volatility_factor': 1.0,     # Market volatility multiplier
            'trend_strength': 0.0,        # -1.0 (down) to 1.0 (up)
            'last_vix_reading': None,
            'last_profile_adjustment': datetime.now(),
            'profile_adjustment_cooldown': timedelta(hours=1)  # Don't adjust too frequently
        }

        # Position rebalancing tracking
        self.rebalancing = {
            'last_rebalance_time': datetime.now(),
            'rebalance_interval_days': 30,     # Rebalance every 30 days
            'allocation_deviation_threshold': 0.05,  # 5% deviation triggers rebalancing
            'demo_rebalance_interval_minutes': 3,    # 3 minutes for demo
            'position_health_checks': {}
        }

        # Enhanced validation statistics
        self.enhanced_stats = {
            'profile_changes': 0,
            'rebalancing_actions': 0,
            'partial_exits': 0,
            'position_additions': 0,
            'avg_confidence_threshold': 0.65,
            'market_condition_adjustments': 0
        }

        self.logger.info("Enhanced Risk Manager initialized with dynamic profiles")
        self.logger.info(f"Starting with {self.current_profile} profile: {self.risk_profiles[self.current_profile]['description']}")

    def validate_decision_enhanced(self, resolved_decision: ResolvedDecision,
                                 market_vix: Optional[float] = None,
                                 current_positions: Optional[Dict[str, float]] = None) -> RiskValidationResult:
        """
        Enhanced validation with dynamic risk profiles and market-based adjustments

        Args:
            resolved_decision: Decision from conflict resolver
            market_vix: Current VIX level for market condition assessment
            current_positions: Current portfolio positions for rebalancing logic

        Returns:
            Enhanced RiskValidationResult with dynamic risk assessment
        """
        start_time = datetime.now()

        try:
            # Step 1: Adjust risk profile based on market conditions
            self._adjust_risk_profile_based_on_market(market_vix)

            # Step 2: Get current active profile settings
            profile = self.risk_profiles[self.current_profile]

            # Step 3: Apply enhanced validation with current profile
            base_result = self.validate_decision(resolved_decision, market_vix)

            # Step 4: Enhanced post-processing based on profile
            if base_result.is_approved:
                # Apply profile-specific confidence threshold
                if resolved_decision.final_confidence < profile['approval_threshold']:
                    reason = f"Confidence {resolved_decision.final_confidence:.2f} below {self.current_profile} threshold {profile['approval_threshold']:.2f}"
                    return self._create_denial_result(resolved_decision, reason)

                # Apply profile-specific position size limits
                if base_result.approved_size_pct and base_result.approved_size_pct > profile['max_position_pct']:
                    # Modify the size instead of denying
                    modified_size = profile['max_position_pct']
                    base_result.approved_size_pct = modified_size
                    base_result.validation_result = ValidationResult.MODIFIED
                    base_result.reason = f"Position size reduced to {self.current_profile} profile limit: {modified_size:.1%}"
                    base_result.warnings.append(f"Size adjusted for {self.current_profile} risk profile")

                    # Recalculate with modified size
                    base_result.size_calculation = self.position_sizer.size_up(resolved_decision.symbol, modified_size)

            # Step 5: Check for rebalancing opportunities
            self._check_rebalancing_opportunities(current_positions, base_result)

            # Step 6: Enhanced logging with ASCII characters only
            self._log_enhanced_decision(resolved_decision, base_result, profile)

            # Step 7: Update enhanced statistics
            self._update_enhanced_stats(base_result)

            return base_result

        except Exception as e:
            self.logger.error(f"Enhanced validation error: {e}")
            return self._create_denial_result(resolved_decision, f"Enhanced validation failed: {e}")

    def _adjust_risk_profile_based_on_market(self, market_vix: Optional[float] = None):
        """
        Dynamically adjust risk profile based on market conditions
        """
        if market_vix is None:
            return

        # Check cooldown period to avoid too frequent changes
        if datetime.now() - self.market_conditions['last_profile_adjustment'] < self.market_conditions['profile_adjustment_cooldown']:
            return

        old_profile = self.current_profile

        # Determine new profile based on VIX and market conditions
        if market_vix > 35:
            # High volatility - go conservative
            new_profile = 'conservative'
            market_state = 'volatile'
        elif market_vix < 20:
            # Low volatility - can be more aggressive
            new_profile = 'aggressive'
            market_state = 'stable'
        else:
            # Moderate volatility - balanced approach
            new_profile = 'moderate'
            market_state = 'balanced'

        # Apply profile change if different
        if new_profile != old_profile:
            self.current_profile = new_profile
            self.market_conditions['last_profile_adjustment'] = datetime.now()
            self.enhanced_stats['profile_changes'] += 1
            self.enhanced_stats['market_condition_adjustments'] += 1

            # Log profile change with ASCII characters
            self.logger.info(f"Risk profile changed: {old_profile} -> {new_profile}")
            self.logger.info(f"Market condition: {market_state} (VIX: {market_vix:.1f})")
            self.logger.info(f"New limits: max_position={self.risk_profiles[new_profile]['max_position_pct']:.1%}, "
                           f"approval_threshold={self.risk_profiles[new_profile]['approval_threshold']:.0%}")

            # Track profile change history
            self.profile_change_history.append({
                'timestamp': datetime.now(),
                'old_profile': old_profile,
                'new_profile': new_profile,
                'vix': market_vix,
                'reason': f'Market condition: {market_state}'
            })

        # Update market condition tracking
        self.market_conditions['last_vix_reading'] = market_vix
        if market_vix > 30:
            self.market_conditions['volatility_factor'] = min(2.0, market_vix / 20.0)
        else:
            self.market_conditions['volatility_factor'] = max(0.5, market_vix / 20.0)

    def _check_rebalancing_opportunities(self, current_positions: Optional[Dict[str, float]],
                                       validation_result: RiskValidationResult):
        """
        Check if portfolio rebalancing is needed
        """
        if not current_positions:
            return

        current_time = datetime.now()

        # Check time-based rebalancing (every 30 days in production, 3 minutes in demo)
        time_since_rebalance = current_time - self.rebalancing['last_rebalance_time']
        demo_threshold = timedelta(minutes=self.rebalancing['demo_rebalance_interval_minutes'])
        production_threshold = timedelta(days=self.rebalancing['rebalance_interval_days'])

        # Use demo threshold for testing, production threshold for live trading
        rebalance_threshold = demo_threshold if len(current_positions) < 10 else production_threshold

        needs_time_rebalance = time_since_rebalance > rebalance_threshold

        # Check allocation-based rebalancing (5% deviation)
        needs_allocation_rebalance = False
        target_allocation = 1.0 / max(len(current_positions), 1)  # Equal weight target

        for symbol, current_pct in current_positions.items():
            deviation = abs(current_pct - target_allocation)
            if deviation > self.rebalancing['allocation_deviation_threshold']:
                needs_allocation_rebalance = True
                break

        # Log rebalancing analysis
        if needs_time_rebalance or needs_allocation_rebalance:
            reasons = []
            if needs_time_rebalance:
                reasons.append(f"time-based ({time_since_rebalance.total_seconds() / 60:.1f} min)")
            if needs_allocation_rebalance:
                reasons.append("allocation-based")

            validation_result.warnings.append(f"Portfolio rebalancing suggested: {', '.join(reasons)}")
            self.logger.info(f"Rebalancing opportunity detected: {', '.join(reasons)}")

    def _log_enhanced_decision(self, decision: ResolvedDecision, result: RiskValidationResult, profile: Dict):
        """
        Enhanced logging with ASCII-only characters for Windows compatibility
        """
        # Use only ASCII characters in log messages
        status = "APPROVED" if result.is_approved else "DENIED"
        confidence_pct = int(decision.final_confidence * 100)

        log_msg = f"RISK {status} - {decision.symbol}: " \
                 f"confidence={confidence_pct}%, " \
                 f"profile={self.current_profile}, " \
                 f"threshold={int(profile['approval_threshold'] * 100)}%"

        if result.approved_size_pct:
            log_msg += f", size={result.approved_size_pct:.1%}"

        if result.warnings:
            log_msg += f", warnings={len(result.warnings)}"

        self.logger.info(log_msg)

        # Log detailed reasoning with ASCII characters
        if not result.is_approved:
            self.logger.info(f"    Denial reason: {result.reason}")
        elif result.warnings:
            for warning in result.warnings:
                self.logger.info(f"    Warning: {warning}")

    def _update_enhanced_stats(self, result: RiskValidationResult):
        """
        Update enhanced statistics tracking
        """
        if result.is_approved:
            if result.validation_result == ValidationResult.MODIFIED:
                self.enhanced_stats['position_additions'] += 1

        # Update average confidence threshold
        current_threshold = self.risk_profiles[self.current_profile]['approval_threshold']
        self.enhanced_stats['avg_confidence_threshold'] = (
            self.enhanced_stats['avg_confidence_threshold'] * 0.9 +
            current_threshold * 0.1
        )

    def get_enhanced_risk_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive risk summary including enhanced features
        """
        base_summary = self.get_risk_summary()

        enhanced_summary = {
            **base_summary,
            'enhanced_features': {
                'current_risk_profile': self.current_profile,
                'profile_description': self.risk_profiles[self.current_profile]['description'],
                'market_conditions': {
                    'volatility_factor': self.market_conditions['volatility_factor'],
                    'last_vix': self.market_conditions['last_vix_reading'],
                    'trend_strength': self.market_conditions['trend_strength']
                },
                'enhanced_stats': self.enhanced_stats,
                'profile_limits': {
                    'approval_threshold': self.risk_profiles[self.current_profile]['approval_threshold'],
                    'max_position_pct': self.risk_profiles[self.current_profile]['max_position_pct'],
                    'max_portfolio_allocation': self.risk_profiles[self.current_profile]['max_portfolio_allocation'],
                    'daily_trade_limit': self.risk_profiles[self.current_profile]['daily_trade_limit']
                },
                'rebalancing_status': {
                    'last_rebalance': self.rebalancing['last_rebalance_time'].isoformat(),
                    'next_rebalance_due': (self.rebalancing['last_rebalance_time'] +
                                         timedelta(minutes=self.rebalancing['demo_rebalance_interval_minutes'])).isoformat()
                }
            }
        }

        return enhanced_summary

    def should_consider_partial_exit(self, symbol: str, current_pct: float,
                                   sentiment_score: float) -> Tuple[bool, float]:
        """
        Determine if a position should be partially exited based on size and sentiment

        Args:
            symbol: Symbol to check
            current_pct: Current position size as percentage
            sentiment_score: Current sentiment score (-1.0 to 1.0)

        Returns:
            Tuple of (should_exit, suggested_exit_percentage)
        """
        profile = self.risk_profiles[self.current_profile]

        # Check if position is large enough to consider partial exit
        large_position_threshold = profile['max_position_pct'] * 0.7  # 70% of max position

        if current_pct < large_position_threshold:
            return False, 0.0

        # Check sentiment conditions for partial exit
        negative_sentiment_threshold = -0.2  # Exit if sentiment < -0.2

        if sentiment_score < negative_sentiment_threshold:
            # Calculate exit percentage based on sentiment strength
            sentiment_strength = abs(sentiment_score)
            exit_pct = min(0.4, sentiment_strength * 0.5)  # Exit 30-40% based on sentiment

            self.logger.info(f"Partial exit suggested for {symbol}: "
                           f"position={current_pct:.1%}, sentiment={sentiment_score:.2f}, "
                           f"exit={exit_pct:.1%}")

            return True, exit_pct

        return False, 0.0

# Add utility function for creating enhanced risk manager
def create_enhanced_risk_manager(position_sizer: Optional[Any] = None,
                               db_manager: Optional[Any] = None,
                               alpaca_interface: Optional[Any] = None) -> EnhancedRiskManager:
    """
    Factory function to create EnhancedRiskManager with proper dependencies
    """
    return EnhancedRiskManager(position_sizer, db_manager, alpaca_interface)
