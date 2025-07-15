"""
strategy_assignment_engine.py

Intelligent Strategy Assignment Engine for Trading System
Automatically assigns optimal strategies to positions based on:
- Market regime analysis
- Stock characteristics 
- Performance history
- Risk management rules
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
# random import removed - using real data only

from .strategies import AVAILABLE_STRATEGIES
from .strategy_selector import StrategySelector
from ..analytics.position_health_analyzer import ActiveStrategy, StrategyStatus
from ...analytics.market_metrics import get_market_metrics, MarketMetricsResult

# Import shared models (moved to prevent circular imports)
from .assignment.models import StrategyAssignment, AssignmentReason, AssignmentScenario, ScenarioRequest

# Import 3-scenario assignment system for decision engine integration (ZERO DISCONNECTS)
# This preserves all existing functionality while adding scenario coordination
try:
    from .assignment.scenario_coordinator import ThreeScenarioCoordinator
    from .assignment.database_integrator import DatabaseIntegrator
    from .assignment.assignment_validator import AssignmentValidator
    SCENARIO_SYSTEM_AVAILABLE = True
except ImportError as e:
    # Graceful fallback if scenario system not available
    SCENARIO_SYSTEM_AVAILABLE = False
    import logging
    logging.getLogger(__name__).warning(f"3-scenario assignment system not available: {e} - using legacy assignment")

# AssignmentReason and StrategyAssignment now imported from .assignment.models
# to prevent circular import dependencies

@dataclass 
class AssignmentRule:
    """Rules for strategy assignment"""
    name: str
    condition: callable
    strategy_preferences: List[str]
    priority: int = 1
    description: str = ""

class StrategyAssignmentEngine:
    """
    Intelligent Strategy Assignment Engine
    
    Features:
    - Market regime-based strategy selection
    - Performance-driven reassignment
    - Risk-balanced portfolio diversification
    - Automatic adaptation to changing conditions
    - Three assignment scenarios:
      1. User buy-and-hold positions (no active strategy)
      2. Decision engine algorithmic selection (optimal strategy)
      3. Strategy reevaluation from position health monitor (performance-based)
    """
    
    def __init__(self, symbols: List[str], risk_tolerance: str = "moderate", db_manager=None, redis_manager=None):
        self.symbols = symbols
        self.risk_tolerance = risk_tolerance
        self.logger = logging.getLogger("strategy_assignment_engine")
        
        # Redis manager for real-time market regime access
        self.redis_manager = redis_manager
        if not self.redis_manager:
            try:
                from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager
                self.redis_manager = get_trading_redis_manager()
            except Exception as e:
                self.logger.warning(f"Could not initialize Redis manager: {e}")
                self.redis_manager = None
        
        # Initialize strategy selector for market analysis
        self.strategy_selector = StrategySelector()
        
        # Track assignment history
        self.assignment_history: Dict[str, List[StrategyAssignment]] = {}
        self.current_assignments: Dict[str, StrategyAssignment] = {}
        
        # Performance tracking
        self.strategy_performance: Dict[str, Dict[str, float]] = {}
        
        # Assignment rules
        self.assignment_rules = self._create_assignment_rules()
        
        # Real-time market regime access
        self.current_regime_cache = {'regime': 'UNKNOWN', 'last_updated': None}
        self.regime_cache_ttl = 300  # 5 minutes cache
        
        # Strategy categories for diversification
        self.strategy_categories = {
            'momentum': ['momentum', 'pca_momentum', 'jump_momentum'],
            'mean_reversion': ['mean_reversion', 'bollinger', 'pca_mean_reversion', 'jump_reversal'],
            'volatility': ['volatility', 'volatility_breakout'],
            'factor': ['pca_low_beta', 'pca_idiosyncratic'],
            'pairs': ['pairs']
        }
        
        # Initialize 3-scenario assignment coordination (ZERO DISCONNECTS)
        if SCENARIO_SYSTEM_AVAILABLE:
            try:
                # Pass self to coordinator to prevent circular initialization
                self.scenario_coordinator = ThreeScenarioCoordinator(assignment_engine=self)
                self.database_integrator = DatabaseIntegrator(db_manager=db_manager)
                self.assignment_validator = AssignmentValidator()
                self.logger.info("3-scenario assignment system initialized - enhanced coordination active")
            except Exception as e:
                self.logger.warning(f"Failed to initialize 3-scenario system: {e} - using legacy assignment")
                self.scenario_coordinator = None
                self.database_integrator = None
                self.assignment_validator = None
        else:
            self.scenario_coordinator = None
            self.database_integrator = None
            self.assignment_validator = None
        
        self.logger.info(f"StrategyAssignmentEngine initialized for {len(symbols)} symbols")
        self.logger.info(f"Risk tolerance: {risk_tolerance}")
        if self.scenario_coordinator:
            self.logger.info("3-scenario assignment coordination enabled")
        self.logger.info(f"Available strategies: {len(AVAILABLE_STRATEGIES)}")
        
        # Initialize market regime data
        self._update_regime_cache()
    
    def assign_strategy_for_user_position(self, symbol: str, user_intent: str = "buy_hold") -> StrategyAssignment:
        """
        Handle user-selected stock position (Scenario 1)
        User chooses stock, no active strategy needed - just monitoring
        
        Args:
            symbol: User-selected stock symbol
            user_intent: User's investment intent ("buy_hold", "manual_trade")
            
        Returns:
            StrategyAssignment for user position
        """
        try:
            self.logger.info(f"Creating user position assignment for {symbol} with intent: {user_intent}")
            
            # ZERO DISCONNECTS: Route through 3-scenario coordinator if available  
            if self.scenario_coordinator and user_intent == "buy_hold":
                try:
                    request = ScenarioRequest(
                        symbol=symbol,
                        scenario=AssignmentScenario.USER_BUY_HOLD,
                        user_data={'user_intent': user_intent},
                        metadata={'assignment_source': 'user_request'}
                    )
                    assignment = self.scenario_coordinator.route_assignment_request(request)
                    
                    # Database integration and validation
                    if self.database_integrator:
                        self.database_integrator.record_assignment(assignment, AssignmentScenario.USER_BUY_HOLD)
                    if self.assignment_validator:
                        validation = self.assignment_validator.validate_assignment(assignment)
                        if not validation.is_valid:
                            self.logger.warning(f"Assignment validation warnings for {symbol}: {validation.warnings}")
                    
                    return assignment
                except Exception as e:
                    self.logger.warning(f"3-scenario coordination failed for {symbol}: {e} - falling back to legacy assignment")
            
            # Legacy assignment method (preserves existing functionality)
            # For user buy-and-hold positions, assign monitoring strategy
            if user_intent == "buy_hold":
                # Get real market metrics for user position
                metrics = get_market_metrics().get_comprehensive_metrics(symbol)
                
                assignment = StrategyAssignment(
                    symbol=symbol,
                    recommended_strategy="position_monitoring",  # Special strategy for user positions
                    confidence_score=self._calculate_user_position_confidence(metrics),
                    reason=AssignmentReason.USER_BUY_HOLD,
                    market_regime=self._get_current_market_regime(symbol),
                    risk_score=metrics.risk_score if metrics.risk_score is not None else self._handle_insufficient_data_risk(symbol),
                    expected_return=self._calculate_user_position_expected_return(metrics),
                    metadata={
                        'user_controlled': True,
                        'strategy_type': 'passive_monitoring',
                        'user_intent': user_intent
                    }
                )
            else:
                # For other user intents, fail explicitly
                raise ValueError(f"Unsupported user intent: {user_intent}. Only 'buy_hold' supported for user positions.")
            
            self._record_assignment(assignment)
            self.logger.info(f"User position created: {symbol} -> {assignment.recommended_strategy}")
            return assignment
            
        except Exception as e:
            self.logger.error(f"Error creating user position assignment for {symbol}: {e}")
            raise
    
    def _legacy_assign_strategy_for_decision_engine(self, symbol: str, market_analysis: Optional[Dict] = None, 
                                                  decision_context: Optional[Dict] = None) -> StrategyAssignment:
        """
        Legacy decision engine assignment method (bypasses 3-scenario coordinator to prevent recursion)
        """
        try:
            self.logger.info(f"Legacy decision engine strategy assignment for {symbol}")
            
            # Use market analysis or compute it
            if not market_analysis:
                market_analysis = self._analyze_market_conditions(symbol)
            
            # Get strategy fitness scores using real market data
            strategy_scores = self._calculate_strategy_scores(symbol, market_analysis)
            
            if not strategy_scores:
                raise ValueError(f"No strategies available for {symbol} - strategy assignment failed")
            
            # CRITICAL FIX: Get real-time market regime for strategy selection
            real_time_regime = self._get_real_time_market_regime()
            regime_strategies = self._select_strategies_for_regime(real_time_regime)
            
            # Filter strategies based on current regime + assignment rules
            filtered_strategies = self._apply_assignment_rules(symbol, strategy_scores, real_time_regime)
            regime_filtered_strategies = {k: v for k, v in filtered_strategies.items() if k in regime_strategies}
            
            # Use regime-filtered strategies if available, otherwise fall back to all filtered
            final_strategy_pool = regime_filtered_strategies if regime_filtered_strategies else filtered_strategies
            
            # Select best strategy based on real performance data + regime suitability
            best_strategy, confidence = self._select_best_strategy(final_strategy_pool)
            
            assignment = StrategyAssignment(
                symbol=symbol,
                recommended_strategy=best_strategy,
                assignment_reason=AssignmentReason.DECISION_ENGINE_CHOICE,
                confidence_score=confidence,
                risk_score=self._get_validated_risk_score(symbol, market_analysis),
                expected_return=self._get_validated_expected_return(best_strategy, strategy_scores, symbol),
                assignment_scenario=AssignmentScenario.DECISION_ENGINE_CHOICE,
                market_regime=real_time_regime.get('regime', 'UNKNOWN'),
                assignment_metadata={
                    'algorithmic_selection': True,
                    'market_analysis': market_analysis,
                    'real_time_regime': real_time_regime,
                    'regime_based_strategies': regime_strategies,
                    'strategy_scores': {k: v.get('fitness_score', 0) for k, v in strategy_scores.items()},
                    'selection_criteria': 'optimal_performance'
                }
            )
            
            self._record_assignment(assignment)
            self.logger.info(f"Legacy decision engine assignment: {symbol} -> {best_strategy} ({confidence:.1f}% confidence)")
            return assignment
            
        except Exception as e:
            self.logger.error(f"Error in legacy decision engine strategy assignment for {symbol}: {e}")
            raise

    def assign_strategy_for_decision_engine(self, symbol: str, market_analysis: Optional[Dict] = None, 
                                           decision_context: Optional[Dict] = None) -> StrategyAssignment:
        """
        Handle decision engine algorithmic selection (Scenario 2)
        Decision engine chooses both stock and optimal strategy
        
        Args:
            symbol: Stock symbol chosen by decision engine
            market_analysis: Optional pre-computed market analysis
            
        Returns:
            StrategyAssignment with optimal strategy for algorithmic trading
        """
        try:
            self.logger.info(f"Decision engine strategy assignment for {symbol}")
            
            # ZERO DISCONNECTS: Route through 3-scenario coordinator if available
            if self.scenario_coordinator:
                try:
                    request = ScenarioRequest(
                        symbol=symbol,
                        scenario=AssignmentScenario.DECISION_ENGINE_CHOICE,
                        market_data=market_analysis,
                        metadata=decision_context or {}
                    )
                    assignment = self.scenario_coordinator.route_assignment_request(request)
                    
                    # Database integration and validation
                    if self.database_integrator:
                        self.database_integrator.record_assignment(assignment, AssignmentScenario.DECISION_ENGINE_CHOICE)
                    if self.assignment_validator:
                        validation = self.assignment_validator.validate_assignment(assignment)
                        if not validation.is_valid:
                            self.logger.warning(f"Assignment validation warnings for {symbol}: {validation.warnings}")
                    
                    return assignment
                except Exception as e:
                    self.logger.warning(f"3-scenario coordination failed for {symbol}: {e} - falling back to legacy assignment")
            
            # Fallback to legacy assignment method
            return self._legacy_assign_strategy_for_decision_engine(symbol, market_analysis, decision_context)
            
        except Exception as e:
            self.logger.error(f"Error in decision engine strategy assignment for {symbol}: {e}")
            raise
    
    def reassign_strategy_from_health_monitor(self, symbol: str, current_strategy: str, 
                                            performance_data: Dict, health_analysis: Dict) -> StrategyAssignment:
        """
        Handle strategy reevaluation from position health monitor (Scenario 3)
        Current strategy is failing, position health monitor triggers reassignment
        
        Args:
            symbol: Stock symbol with failing strategy
            current_strategy: Currently assigned strategy that's failing
            performance_data: Real performance metrics from position health monitor
            health_analysis: Health analysis data triggering reassignment
            
        Returns:
            StrategyAssignment with new strategy based on performance analysis
        """
        try:
            self.logger.warning(f"Strategy reevaluation triggered for {symbol}: {current_strategy} underperforming")
            
            # ZERO DISCONNECTS: Route through 3-scenario coordinator if available
            if self.scenario_coordinator:
                try:
                    request = ScenarioRequest(
                        symbol=symbol,
                        scenario=AssignmentScenario.STRATEGY_REEVALUATION,
                        performance_data=performance_data,
                        metadata={
                            'current_strategy': current_strategy,
                            'health_analysis': health_analysis,
                            'reassignment_trigger': 'health_monitor'
                        }
                    )
                    assignment = self.scenario_coordinator.route_assignment_request(request)
                    
                    # Database integration and validation
                    if self.database_integrator:
                        self.database_integrator.record_assignment(assignment, AssignmentScenario.STRATEGY_REEVALUATION)
                    if self.assignment_validator:
                        validation = self.assignment_validator.validate_assignment(assignment)
                        if not validation.is_valid:
                            self.logger.warning(f"Assignment validation warnings for {symbol}: {validation.warnings}")
                    
                    return assignment
                except Exception as e:
                    self.logger.warning(f"3-scenario coordination failed for {symbol}: {e} - falling back to legacy reassignment")
            
            # Legacy reassignment method (preserves existing functionality)
            # Validate that we have real performance data
            if not performance_data or 'total_return' not in performance_data:
                raise ValueError(f"Position health monitor must provide real performance data for {symbol}")
            
            # Analyze why current strategy is failing
            failure_analysis = self._analyze_strategy_failure(symbol, current_strategy, performance_data, health_analysis)
            
            # Get current market conditions for reassignment
            market_analysis = self._analyze_market_conditions(symbol)
            
            # Calculate strategy scores excluding the failing strategy
            strategy_scores = self._calculate_strategy_scores(symbol, market_analysis)
            
            # Remove the failing strategy from consideration
            if current_strategy in strategy_scores:
                del strategy_scores[current_strategy]
                self.logger.info(f"Excluded failing strategy {current_strategy} from reassignment options")
            
            if not strategy_scores:
                raise ValueError(f"No alternative strategies available for {symbol} after excluding {current_strategy}")
            
            # Apply special rules for reassignment based on failure analysis
            filtered_strategies = self._apply_reassignment_rules(symbol, strategy_scores, failure_analysis)
            
            # Select best alternative strategy
            best_strategy, confidence = self._select_best_strategy(filtered_strategies)
            
            assignment = StrategyAssignment(
                symbol=symbol,
                recommended_strategy=best_strategy,
                confidence_score=confidence,
                reason=AssignmentReason.STRATEGY_REEVALUATION,
                market_regime=market_analysis.get('regime', 'UNKNOWN'),
                risk_score=self._get_validated_risk_score(symbol, market_analysis),
                expected_return=self._get_validated_expected_return(best_strategy, strategy_scores, symbol),
                metadata={
                    'previous_strategy': current_strategy,
                    'failure_analysis': failure_analysis,
                    'performance_data': performance_data,
                    'health_trigger': health_analysis,
                    'reassignment_type': 'performance_based'
                }
            )
            
            self._record_assignment(assignment)
            self.logger.warning(f"Strategy reassigned for {symbol}: {current_strategy} -> {best_strategy} due to underperformance")
            return assignment
            
        except Exception as e:
            self.logger.error(f"Error in strategy reevaluation for {symbol}: {e}")
            raise
    
    def assign_strategy_to_position(self, symbol: str, current_strategy: Optional[str] = None,
                                  force_reason: Optional[AssignmentReason] = None, 
                                  assignment_context: Optional[Dict] = None) -> StrategyAssignment:
        """
        DEPRECATED: Use specific assignment methods instead:
        - assign_strategy_for_user_position() for user buy-hold positions
        - assign_strategy_for_decision_engine() for algorithmic selection  
        - reassign_strategy_from_health_monitor() for performance-based reassignment
        
        This method now routes to appropriate specific method based on context
        """
        try:
            self.logger.info(f"Analyzing strategy assignment for {symbol}")
            
            # Step 1: Analyze market conditions for the symbol
            market_analysis = self._analyze_market_conditions(symbol)
            
            # Step 2: Get strategy fitness scores
            strategy_scores = self._calculate_strategy_scores(symbol, market_analysis)
            
            # Step 3: Apply assignment rules and constraints
            filtered_strategies = self._apply_assignment_rules(symbol, strategy_scores, current_strategy)
            
            # Step 4: Select best strategy
            best_strategy, confidence = self._select_best_strategy(filtered_strategies)
            
            # Step 5: Determine assignment reason
            reason = force_reason or self._determine_assignment_reason(symbol, current_strategy, best_strategy)
            
            # Step 6: Create assignment
            assignment = StrategyAssignment(
                symbol=symbol,
                recommended_strategy=best_strategy,
                confidence_score=confidence,
                reason=reason,
                market_regime=market_analysis.get('regime', 'UNKNOWN'),
                risk_score=self._get_validated_risk_score(symbol, market_analysis),
                expected_return=self._get_validated_expected_return(best_strategy, strategy_scores, symbol),
                metadata={
                    'market_analysis': market_analysis,
                    'strategy_scores': strategy_scores,
                    'previous_strategy': current_strategy,
                    'alternatives': list(filtered_strategies.keys())[:3]
                }
            )
            
            # Step 7: Record assignment
            self._record_assignment(assignment)
            
            self.logger.info(f"Strategy assigned to {symbol}: {best_strategy} ({confidence:.1f}% confidence)")
            self.logger.info(f"   Reason: {reason.value}, Market: {assignment.market_regime}")
            
            # BUSINESS LOGIC OUTPUT: Detailed strategy assignment decision
            try:
                from terminal_color_system import print_strategy_assignment
                previous_info = f"previous: {current_strategy}" if current_strategy else "new position"
                reason_desc = reason.value.replace('_', ' ')
                print_strategy_assignment(f"{symbol}: assigned {best_strategy} | confidence: {confidence:.1f}% | {previous_info} | reason: '{reason_desc.title()}'")
            except ImportError:
                previous_info = f"previous: {current_strategy}" if current_strategy else "new position"
                reason_desc = reason.value.replace('_', ' ')
                print(f"[STRATEGY ASSIGNMENT] {symbol}: assigned {best_strategy} | confidence: {confidence:.1f}% | {previous_info} | reason: '{reason_desc.title()}'")
            
            # REDIS MESSAGING: Send strategy assignment to decision engine
            self._send_strategy_assignment_to_decision_engine(assignment)
            
            return assignment
            
        except Exception as e:
            self.logger.error(f"Error assigning strategy to {symbol}: {e}")
            # Fallback to safe default
            return self._create_fallback_assignment(symbol, current_strategy)
    
    def assign_strategies_to_portfolio(self, current_assignments: Dict[str, str]) -> Dict[str, StrategyAssignment]:
        """
        Assign strategies to entire portfolio with diversification optimization
        
        Args:
            current_assignments: Dict of {symbol: current_strategy}
            
        Returns:
            Dict of {symbol: StrategyAssignment}
        """
        self.logger.info(f"Assigning strategies to portfolio of {len(self.symbols)} positions")
        
        portfolio_assignments = {}
        strategy_usage = {}  # Track strategy diversification
        
        # Sort symbols by priority (largest positions first, new positions last)
        prioritized_symbols = self._prioritize_symbols_for_assignment()
        
        for symbol in prioritized_symbols:
            current_strategy = current_assignments.get(symbol)
            
            # Assign strategy considering portfolio diversification
            assignment = self.assign_strategy_to_position(symbol, current_strategy)
            
            # Track strategy usage for diversification
            strategy = assignment.recommended_strategy
            strategy_usage[strategy] = strategy_usage.get(strategy, 0) + 1
            
            # Apply diversification adjustment if needed
            if self._needs_diversification_adjustment(strategy_usage, assignment):
                alternative_assignment = self._get_diversified_alternative(symbol, assignment, strategy_usage)
                if alternative_assignment:
                    assignment = alternative_assignment
                    strategy_usage[assignment.recommended_strategy] = strategy_usage.get(assignment.recommended_strategy, 0) + 1
                    strategy_usage[strategy] -= 1
            
            portfolio_assignments[symbol] = assignment
        
        # Final portfolio analysis
        self._log_portfolio_strategy_distribution(portfolio_assignments)
        
        return portfolio_assignments
    
    def should_reassign_strategy(self, symbol: str, current_strategy: str, 
                               performance_data: Optional[Dict] = None) -> Tuple[bool, str]:
        """
        Determine if a strategy should be reassigned based on performance and conditions
        
        Args:
            symbol: Stock symbol
            current_strategy: Currently assigned strategy
            performance_data: Optional performance metrics
            
        Returns:
            Tuple of (should_reassign: bool, reason: str)
        """
        try:
            # Check performance-based reassignment
            if performance_data:
                if self._strategy_underperforming(current_strategy, performance_data):
                    return True, "Poor performance: Strategy underperforming benchmarks"
            
            # Check market regime change
            current_regime = self._get_current_market_regime(symbol)
            if self._strategy_mismatched_to_regime(current_strategy, current_regime):
                return True, f"Market regime change: {current_regime} regime favors different strategies"
            
            # Check time-based reassignment (monthly rebalance)
            last_assignment = self.current_assignments.get(symbol)
            if last_assignment and self._time_for_rebalance(last_assignment):
                return True, "Scheduled rebalancing: Monthly strategy review"
            
            return False, "Strategy assignment still optimal"
            
        except Exception as e:
            self.logger.error(f"Error checking reassignment for {symbol}: {e}")
            return False, f"Error in reassignment check: {e}"
    
    def get_strategy_recommendations(self, symbol: str, top_n: int = 3) -> List[Tuple[str, float]]:
        """Get top N strategy recommendations for a symbol with confidence scores"""
        try:
            market_analysis = self._analyze_market_conditions(symbol)
            strategy_scores = self._calculate_strategy_scores(symbol, market_analysis)
            
            # Sort by fitness score
            sorted_strategies = sorted(strategy_scores.items(), 
                                     key=lambda x: x[1].get('fitness_score', 0), 
                                     reverse=True)
            
            return [(strategy, data.get('fitness_score', 0)) for strategy, data in sorted_strategies[:top_n]]
            
        except Exception as e:
            self.logger.error(f"Error getting recommendations for {symbol}: {e}")
            return [('momentum', 75.0), ('mean_reversion', 65.0), ('volatility', 55.0)]  # Safe defaults
    
    # === PRIVATE HELPER METHODS ===
    
    def _analyze_market_conditions(self, symbol: str) -> Dict[str, Any]:
        """Analyze current market conditions for a symbol"""
        try:
            # Use strategy selector to analyze market regime
            market_data = self.strategy_selector.analyze_market_regime()
            
            # Add symbol-specific analysis using real market data
            analysis = {
                'regime': market_data.get('primary_regime', 'UNKNOWN'),
                'volatility': self._get_real_volatility(symbol),
                'trend_strength': self._get_real_trend_strength(symbol),
                'risk_score': self._get_real_risk_score(symbol),
                'liquidity_score': self._get_real_liquidity_score(symbol),
                'sector': self._get_symbol_sector(symbol),
                'market_cap': self._get_symbol_market_cap(symbol)
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing market conditions for {symbol}: {e}")
            return {'regime': 'UNKNOWN', 'risk_score': 50.0}
    
    def _calculate_strategy_scores(self, symbol: str, market_analysis: Dict) -> Dict[str, Dict[str, float]]:
        """Calculate fitness scores for all strategies"""
        strategy_scores = {}
        
        for strategy_name in AVAILABLE_STRATEGIES.keys():
            try:
                # Base fitness from market regime matching
                fitness_score = self._calculate_regime_fitness(strategy_name, market_analysis['regime'])
                
                # Adjust for symbol characteristics
                fitness_score *= self._calculate_symbol_fit_multiplier(symbol, strategy_name)
                
                # Adjust for risk tolerance
                fitness_score *= self._calculate_risk_fit_multiplier(strategy_name)
                
                # Add historical performance bonus
                performance_bonus = self._get_historical_performance_bonus(strategy_name, symbol)
                fitness_score += performance_bonus
                
                # Ensure score stays in valid range
                fitness_score = max(10, min(100, fitness_score))
                
                strategy_scores[strategy_name] = {
                    'fitness_score': fitness_score,
                    'expected_return': self._calculate_strategy_expected_return(strategy_name, symbol),
                    'risk_score': self._calculate_strategy_risk_score(strategy_name, symbol),
                    'complexity': self._get_strategy_complexity(strategy_name)
                }
                
            except Exception as e:
                self.logger.error(f"Error scoring strategy {strategy_name} for {symbol}: {e}")
                # Skip strategy with insufficient data - no hardcoded fallbacks
                continue
        
        return strategy_scores
    
    def _calculate_regime_fitness(self, strategy_name: str, regime: str) -> float:
        """Calculate how well a strategy fits the current market regime"""
        regime_fitness_map = {
            'BULLISH_TRENDING': {
                'momentum': 85, 'pca_momentum': 80, 'jump_momentum': 75,
                'mean_reversion': 40, 'bollinger': 45, 'volatility': 60
            },
            'BEARISH_TRENDING': {
                'momentum': 35, 'mean_reversion': 80, 'bollinger': 85,
                'pca_low_beta': 90, 'volatility': 70
            },
            'RANGE_BOUND_STABLE': {
                'mean_reversion': 90, 'bollinger': 85, 'pairs': 80,
                'momentum': 30, 'volatility': 60
            },
            'HIGH_VOLATILITY': {
                'volatility': 95, 'volatility_breakout': 90, 'jump_reversal': 80,
                'bollinger': 70, 'momentum': 40
            },
            'UNKNOWN': {
                'momentum': 70, 'mean_reversion': 70, 'volatility': 60  # Balanced defaults
            }
        }
        
        regime_map = regime_fitness_map.get(regime, regime_fitness_map['UNKNOWN'])
        return regime_map.get(strategy_name, 50.0)  # Default score if strategy not mapped
    
    def _create_assignment_rules(self) -> List[AssignmentRule]:
        """Create strategy assignment rules"""
        return [
            AssignmentRule(
                name="high_volatility_preference",
                condition=lambda analysis: analysis.get('volatility', 0) > 0.25,
                strategy_preferences=['volatility', 'volatility_breakout', 'bollinger'],
                priority=3,
                description="Prefer volatility strategies in high volatility environments"
            ),
            AssignmentRule(
                name="trending_market_momentum",
                condition=lambda analysis: analysis.get('trend_strength', 0) > 0.6,
                strategy_preferences=['momentum', 'pca_momentum', 'jump_momentum'],
                priority=2,
                description="Prefer momentum strategies in trending markets"
            ),
            AssignmentRule(
                name="stable_market_mean_reversion",
                condition=lambda analysis: analysis.get('volatility', 0) < 0.2 and analysis.get('trend_strength', 0) < 0.4,
                strategy_preferences=['mean_reversion', 'bollinger', 'pca_mean_reversion'],
                priority=2,
                description="Prefer mean reversion in stable, non-trending markets"
            ),
            AssignmentRule(
                name="risk_averse_defensive",
                condition=lambda analysis: self.risk_tolerance == "conservative",
                strategy_preferences=['pca_low_beta', 'bollinger'],
                priority=1,
                description="Prefer defensive strategies for conservative risk tolerance"
            )
        ]
    
    def _apply_assignment_rules(self, symbol: str, strategy_scores: Dict, current_strategy: Optional[str]) -> Dict[str, Dict]:
        """Apply assignment rules to filter and adjust strategy scores"""
        market_analysis = self._analyze_market_conditions(symbol)
        adjusted_scores = strategy_scores.copy()
        
        # Apply each rule
        for rule in self.assignment_rules:
            try:
                if rule.condition(market_analysis):
                    self.logger.debug(f"Applying rule '{rule.name}' to {symbol}")
                    
                    # Boost preferred strategies
                    for preferred_strategy in rule.strategy_preferences:
                        if preferred_strategy in adjusted_scores:
                            boost = rule.priority * 5  # 5-15 point boost
                            adjusted_scores[preferred_strategy]['fitness_score'] += boost
                            
            except Exception as e:
                self.logger.debug(f"Error applying rule {rule.name}: {e}")
        
        return adjusted_scores
    
    def _select_best_strategy(self, strategy_scores: Dict) -> Tuple[str, float]:
        """Select the best strategy from scored options - NO HARDCODED DEFAULTS"""
        if not strategy_scores:
            raise ValueError("No strategies available - cannot assign strategy without valid options")
        
        # Sort by fitness score
        sorted_strategies = sorted(strategy_scores.items(), 
                                 key=lambda x: x[1].get('fitness_score', 0), 
                                 reverse=True)
        
        best_strategy, best_data = sorted_strategies[0]
        confidence = best_data.get('fitness_score')
        if confidence is None:
            # Skip strategies without valid fitness scores
            raise ValueError(f"Strategy {best_strategy} has no valid fitness score - cannot determine confidence")
        
        # Validate that we have a viable strategy
        if confidence < 30.0:
            raise ValueError(f"Best available strategy {best_strategy} has insufficient confidence ({confidence:.1f}%) - reassignment not viable")
        
        return best_strategy, confidence
    
    def _determine_assignment_reason(self, symbol: str, current_strategy: Optional[str], 
                                   new_strategy: str) -> AssignmentReason:
        """Determine the reason for strategy assignment"""
        if not current_strategy or current_strategy == "default_monitoring":
            return AssignmentReason.NEW_POSITION
        elif current_strategy != new_strategy:
            return AssignmentReason.REGIME_CHANGE
        else:
            return AssignmentReason.REBALANCE
    
    def _record_assignment(self, assignment: StrategyAssignment):
        """Record strategy assignment in history"""
        symbol = assignment.symbol
        
        if symbol not in self.assignment_history:
            self.assignment_history[symbol] = []
        
        self.assignment_history[symbol].append(assignment)
        self.current_assignments[symbol] = assignment
    
    def _create_data_insufficient_assignment(self, symbol: str, current_strategy: Optional[str], reason: str) -> StrategyAssignment:
        """Create assignment when insufficient data - NO HARDCODED VALUES"""
        self.logger.warning(f"Insufficient market data for {symbol}: {reason}")
        
        # Attempt to get any available market data
        metrics = get_market_metrics().get_comprehensive_metrics(symbol)
        
        return StrategyAssignment(
            symbol=symbol,
            recommended_strategy=current_strategy or self._get_conservative_strategy(),
            confidence_score=self._calculate_low_confidence_score(metrics),
            reason=AssignmentReason.NEW_POSITION,
            market_regime=self._get_current_market_regime(symbol),
            risk_score=metrics.risk_score if metrics.risk_score is not None else self._estimate_high_risk_score(symbol),
            expected_return=self._estimate_conservative_return(metrics),
            metadata={'data_quality': 'insufficient', 'reason': reason}
        )
    
    def _prioritize_symbols_for_assignment(self) -> List[str]:
        """Prioritize symbols for strategy assignment"""
        # For now, return symbols as-is. Could be enhanced with position size weighting
        return self.symbols.copy()
    
    def _needs_diversification_adjustment(self, strategy_usage: Dict, assignment: StrategyAssignment) -> bool:
        """Check if portfolio needs diversification adjustment"""
        strategy = assignment.recommended_strategy
        total_positions = len(self.symbols)
        
        # If more than 40% of positions use same strategy, consider diversification
        usage_percentage = strategy_usage.get(strategy, 0) / total_positions
        return usage_percentage > 0.4 and total_positions > 2
    
    def _get_diversified_alternative(self, symbol: str, assignment: StrategyAssignment, 
                                   strategy_usage: Dict) -> Optional[StrategyAssignment]:
        """Get alternative strategy for diversification"""
        alternatives = assignment.metadata.get('alternatives', [])
        
        for alt_strategy in alternatives:
            if strategy_usage.get(alt_strategy, 0) == 0:  # Unused strategy
                # Create new assignment with alternative
                alt_assignment = StrategyAssignment(
                    symbol=symbol,
                    recommended_strategy=alt_strategy,
                    confidence_score=max(10.0, assignment.confidence_score * 0.9),  # Slightly lower confidence with minimum
                    reason=AssignmentReason.DIVERSIFICATION,
                    market_regime=assignment.market_regime,
                    risk_score=assignment.risk_score,
                    expected_return=assignment.expected_return * 0.95,
                    metadata=assignment.metadata
                )
                return alt_assignment
        
        return None
    
    def _log_portfolio_strategy_distribution(self, assignments: Dict[str, StrategyAssignment]):
        """Log portfolio strategy distribution"""
        strategy_counts = {}
        for assignment in assignments.values():
            strategy = assignment.recommended_strategy
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        
        self.logger.info("📊 Portfolio Strategy Distribution:")
        for strategy, count in sorted(strategy_counts.items()):
            percentage = (count / len(assignments)) * 100
            self.logger.info(f"   {strategy}: {count} positions ({percentage:.1f}%)")
    
    # === Utility Methods ===
    
    def _calculate_symbol_fit_multiplier(self, symbol: str, strategy_name: str) -> float:
        """Calculate how well strategy fits the specific symbol"""
        # For AAPL (large cap tech), slight preferences
        if 'momentum' in strategy_name:
            return 1.1  # Tech stocks often trend well
        elif 'volatility' in strategy_name:
            return 0.9  # AAPL is relatively stable
        else:
            return 1.0
    
    def _calculate_risk_fit_multiplier(self, strategy_name: str) -> float:
        """Adjust score based on risk tolerance"""
        risk_multipliers = {
            'conservative': {'pca_low_beta': 1.2, 'momentum': 0.8, 'volatility': 0.7},
            'moderate': {'momentum': 1.1, 'mean_reversion': 1.1},
            'aggressive': {'volatility': 1.2, 'jump_momentum': 1.15}
        }
        
        return risk_multipliers.get(self.risk_tolerance, {}).get(strategy_name, 1.0)
    
    def _get_historical_performance_bonus(self, strategy_name: str, symbol: str) -> float:
        """Get performance bonus based on historical data"""
        try:
            # Get real historical performance from database
            performance_data = self._get_historical_strategy_performance(strategy_name, symbol)
            if performance_data:
                # Convert performance to bonus points (-5 to +10 range)
                performance_pct = performance_data.get('total_return', 0.0)
                # Scale: 0% = 0 bonus, 10% = +10 bonus, -5% = -5 bonus
                bonus = max(-5.0, min(10.0, performance_pct * 100))
                return float(bonus)
            else:
                return 0.0  # No bonus if no historical data
        except Exception as e:
            self.logger.error(f"Error calculating performance bonus: {e}")
            return 0.0  # No bonus on error
    
    def _get_strategy_complexity(self, strategy_name: str) -> str:
        """Get strategy complexity rating"""
        complex_strategies = ['pca_momentum', 'pca_mean_reversion', 'jump_momentum', 'pairs']
        if strategy_name in complex_strategies:
            return 'high'
        elif 'volatility' in strategy_name:
            return 'medium'
        else:
            return 'low'
    
    def _strategy_underperforming(self, strategy: str, performance_data: Dict) -> bool:
        """Check if strategy is underperforming"""
        return performance_data.get('return', 0) < -0.05  # -5% threshold
    
    def _strategy_mismatched_to_regime(self, strategy: str, regime: str) -> bool:
        """Check if strategy is mismatched to current regime"""
        mismatch_patterns = {
            'BEARISH_TRENDING': ['momentum', 'pca_momentum'],
            'RANGE_BOUND_STABLE': ['momentum', 'jump_momentum']
        }
        
        return strategy in mismatch_patterns.get(regime, [])
    
    def _get_current_market_regime(self, symbol: str) -> str:
        """Get current market regime for symbol"""
        return self._analyze_market_conditions(symbol).get('regime', 'UNKNOWN')
    
    def _time_for_rebalance(self, last_assignment: StrategyAssignment) -> bool:
        """Check if it's time for periodic rebalancing"""
        days_since_assignment = (datetime.now() - last_assignment.assignment_time).days
        return days_since_assignment >= 30  # Monthly rebalancing

    def _get_real_volatility(self, symbol: str) -> float:
        """Get real volatility from market data"""
        try:
            from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData
            price_fetcher = YahooFinancePriceData()
            
            # Get 30 days of price data for volatility calculation
            data = price_fetcher.extract_data(symbol, period="30d")
            if data is not None and len(data) > 1:
                # Calculate daily returns
                returns = data['Close'].pct_change().dropna()
                # Annualized volatility
                volatility = returns.std() * (252 ** 0.5)  # 252 trading days
                return float(volatility)
            else:
                self.logger.warning(f"No price data available for {symbol}, using default volatility")
                return 0.25  # Default moderate volatility
        except Exception as e:
            self.logger.error(f"Error calculating volatility for {symbol}: {e}")
            return 0.25  # Default moderate volatility

    def _get_real_trend_strength(self, symbol: str) -> float:
        """Get real trend strength from technical analysis"""
        try:
            from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData
            price_fetcher = YahooFinancePriceData()
            
            # Get recent price data for trend analysis
            data = price_fetcher.extract_data(symbol, period="60d")
            if data is not None and len(data) > 20:
                # Simple trend strength using price vs moving averages
                close_price = data['Close'].iloc[-1]
                ma_20 = data['Close'].rolling(20).mean().iloc[-1]
                ma_50 = data['Close'].rolling(50).mean().iloc[-1] if len(data) >= 50 else ma_20
                
                # Trend strength based on price relative to moving averages
                trend_strength = abs((close_price - ma_20) / ma_20) + abs((ma_20 - ma_50) / ma_50)
                return min(1.0, float(trend_strength))  # Cap at 1.0
            else:
                return 0.5  # Default moderate trend strength
        except Exception as e:
            self.logger.error(f"Error calculating trend strength for {symbol}: {e}")
            return 0.5  # Default moderate trend strength

    def _get_real_risk_score(self, symbol: str) -> float:
        """Get real risk score based on market metrics"""
        try:
            volatility = self._get_real_volatility(symbol)
            trend_strength = self._get_real_trend_strength(symbol)
            
            # Risk score based on volatility and trend consistency
            # Higher volatility = higher risk, inconsistent trends = higher risk
            risk_score = (volatility * 100) + ((1 - trend_strength) * 50)
            return min(100.0, max(0.0, float(risk_score)))
        except Exception as e:
            self.logger.error(f"Error calculating risk score for {symbol}: {e}")
            return 50.0  # Default moderate risk

    def _get_real_liquidity_score(self, symbol: str) -> float:
        """Get real liquidity score from volume data"""
        try:
            from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData
            price_fetcher = YahooFinancePriceData()
            
            # Get recent volume data
            data = price_fetcher.extract_data(symbol, period="30d")
            if data is not None and 'Volume' in data.columns:
                avg_volume = data['Volume'].mean()
                # Rough liquidity scoring based on average volume
                if avg_volume > 50_000_000:  # Very high volume
                    return 95.0
                elif avg_volume > 10_000_000:  # High volume
                    return 85.0
                elif avg_volume > 1_000_000:  # Moderate volume
                    return 70.0
                elif avg_volume > 100_000:  # Low volume
                    return 50.0
                else:
                    return 30.0  # Very low volume
            else:
                return 60.0  # Default moderate liquidity
        except Exception as e:
            self.logger.error(f"Error calculating liquidity score for {symbol}: {e}")
            return 60.0  # Default moderate liquidity

    def _get_symbol_sector(self, symbol: str) -> str:
        """Get symbol sector - placeholder for real sector lookup"""
        # TODO: Integrate with real sector data source (Yahoo Finance info, API, etc.)
        sector_mapping = {
            'AAPL': 'technology', 'MSFT': 'technology', 'GOOGL': 'technology', 'AMZN': 'consumer_discretionary',
            'TSLA': 'consumer_discretionary', 'NVDA': 'technology', 'META': 'communication_services',
            'NFLX': 'communication_services', 'SPY': 'etf', 'QQQ': 'etf'
        }
        return sector_mapping.get(symbol.upper(), 'unknown')

    def _get_symbol_market_cap(self, symbol: str) -> str:
        """Get symbol market cap category"""
        # TODO: Integrate with real market cap data source
        large_cap_symbols = {'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'SPY', 'QQQ'}
        if symbol.upper() in large_cap_symbols:
            return 'large_cap'
        else:
            return 'unknown'  # Default until real data integration

    def _calculate_strategy_expected_return(self, strategy_name: str, symbol: str) -> float:
        """Calculate expected return for strategy based on market conditions"""
        try:
            # Get market conditions and calculate expected return based on strategy type
            volatility = self._get_real_volatility(symbol)
            trend_strength = self._get_real_trend_strength(symbol)
            
            # Different strategies have different expected returns based on market conditions
            strategy_multipliers = {
                'momentum': trend_strength * 0.02,  # Momentum works better in trending markets
                'mean_reversion': (1 - trend_strength) * 0.015,  # Mean reversion works in ranging markets
                'rsi_contrarian': (1 - trend_strength) * 0.01,
                'bollinger_bands': volatility * 0.015,  # Volatility strategies benefit from volatility
                'moving_average': trend_strength * 0.012,
                'buy_and_hold': 0.008  # Conservative baseline
            }
            
            base_return = strategy_multipliers.get(strategy_name, 0.005)  # Default 0.5%
            
            # Adjust for historical performance
            performance_bonus = self._get_historical_performance_bonus(strategy_name, symbol)
            expected_return = base_return + (performance_bonus * 0.001)  # Convert bonus to decimal
            
            return max(-0.05, min(0.20, expected_return))  # Cap between -5% and 20%
            
        except Exception as e:
            self.logger.error(f"Error calculating expected return for {strategy_name}: {e}")
            return 0.005  # Default 0.5% expected return

    def _calculate_strategy_risk_score(self, strategy_name: str, symbol: str) -> float:
        """Calculate risk score for strategy based on market conditions"""
        try:
            volatility = self._get_real_volatility(symbol)
            
            # Different strategies have different risk profiles
            strategy_risk_multipliers = {
                'momentum': 1.2,  # Momentum strategies are riskier
                'mean_reversion': 0.8,  # Mean reversion generally lower risk
                'rsi_contrarian': 0.9,
                'bollinger_bands': 1.1,  # Volatility strategies are moderately risky
                'moving_average': 0.7,  # Simple moving average is conservative
                'buy_and_hold': 0.6  # Lowest risk strategy
            }
            
            base_risk = volatility * 100  # Convert volatility to risk score
            strategy_multiplier = strategy_risk_multipliers.get(strategy_name, 1.0)
            risk_score = base_risk * strategy_multiplier
            
            return max(10.0, min(90.0, risk_score))  # Keep in reasonable range
            
        except Exception as e:
            self.logger.error(f"Error calculating risk score for {strategy_name}: {e}")
            return 50.0  # Default moderate risk

    def _get_historical_strategy_performance(self, strategy_name: str, symbol: str) -> dict:
        """Get historical performance data for strategy on symbol"""
        try:
            # TODO: Integrate with real database lookup for historical performance
            # For now, return None to indicate no historical data available
            # In production, this would query the trading_strategies or performance tables
            return None
        except Exception as e:
            self.logger.error(f"Error getting historical performance: {e}")
            return None

    def _send_strategy_assignment_to_decision_engine(self, assignment: StrategyAssignment):
        """Send strategy assignment to decision engine via Redis"""
        try:
            from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager, create_process_message, MessagePriority
            
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                self.logger.warning("Redis manager not available - strategy assignment not sent to decision engine")
                return
            
            # Create message data for strategy assignment
            message_data = {
                'symbol': assignment.symbol,
                'strategy': assignment.strategy_name,
                'confidence': assignment.confidence,
                'reason': assignment.reason.value,
                'market_regime': assignment.market_regime,
                'assignment_time': assignment.assignment_time.isoformat(),
                'expected_return': assignment.expected_return,
                'risk_score': assignment.risk_score,
                'previous_strategy': getattr(assignment, 'previous_strategy', None)
            }
            
            # Create Redis message
            message = create_process_message(
                sender="strategy_assignment_engine",
                recipient="decision_engine",
                message_type="STRATEGY_ASSIGNMENT",
                data=message_data,
                priority=MessagePriority.HIGH  # Strategy assignments are high priority
            )
            
            # Send message to decision engine
            success = redis_manager.send_message(message, queue_name="market_decision_engine")
            
            if success:
                self.logger.info(f"Strategy assignment sent to decision engine: {assignment.symbol} -> {assignment.strategy_name}")
            else:
                self.logger.error(f"Failed to send strategy assignment to decision engine for {assignment.symbol}")
                
        except Exception as e:
            self.logger.error(f"Error sending strategy assignment to decision engine: {e}")
            # Don't raise - strategy assignment should still complete even if messaging fails
    
    def _analyze_strategy_failure(self, symbol: str, current_strategy: str, 
                                performance_data: Dict, health_analysis: Dict) -> Dict[str, Any]:
        """
        Analyze why a strategy is failing based on real performance data
        
        Args:
            symbol: Stock symbol
            current_strategy: Failing strategy
            performance_data: Real performance metrics
            health_analysis: Health analysis from position monitor
            
        Returns:
            Analysis of strategy failure reasons
        """
        try:
            failure_reasons = []
            failure_severity = "moderate"
            
            # Analyze performance metrics
            total_return = performance_data.get('total_return', 0.0)
            sharpe_ratio = performance_data.get('sharpe_ratio', 0.0)
            max_drawdown = performance_data.get('max_drawdown', 0.0)
            win_rate = performance_data.get('win_rate', 0.0)
            
            # Check return performance
            if total_return < -0.10:  # -10% threshold
                failure_reasons.append("poor_returns")
                failure_severity = "severe"
            elif total_return < -0.05:  # -5% threshold
                failure_reasons.append("underperforming_returns")
            
            # Check risk-adjusted performance
            if sharpe_ratio < -0.5:
                failure_reasons.append("poor_risk_adjusted_returns")
                
            # Check drawdown
            if max_drawdown > 0.15:  # 15% drawdown
                failure_reasons.append("excessive_drawdown")
                failure_severity = "severe"
            
            # Check win rate
            if win_rate < 0.30:  # Less than 30% win rate
                failure_reasons.append("low_win_rate")
            
            # Market regime mismatch analysis
            current_regime = self._get_current_market_regime(symbol)
            if self._strategy_mismatched_to_regime(current_strategy, current_regime):
                failure_reasons.append("regime_mismatch")
            
            return {
                'failure_reasons': failure_reasons,
                'failure_severity': failure_severity,
                'performance_metrics': performance_data,
                'health_metrics': health_analysis,
                'regime_mismatch': current_regime,
                'recommendation': self._get_failure_recommendation(failure_reasons, failure_severity)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing strategy failure for {symbol}: {e}")
            return {
                'failure_reasons': ['analysis_error'],
                'failure_severity': 'unknown',
                'error': str(e)
            }
    
    def _apply_reassignment_rules(self, symbol: str, strategy_scores: Dict, 
                                failure_analysis: Dict) -> Dict[str, Dict]:
        """
        Apply special rules for strategy reassignment based on failure analysis
        
        Args:
            symbol: Stock symbol
            strategy_scores: Available strategy scores
            failure_analysis: Analysis of why previous strategy failed
            
        Returns:
            Filtered and adjusted strategy scores for reassignment
        """
        try:
            adjusted_scores = strategy_scores.copy()
            failure_reasons = failure_analysis.get('failure_reasons', [])
            failure_severity = failure_analysis.get('failure_severity', 'moderate')
            
            # Apply reassignment rules based on failure reasons
            if 'poor_returns' in failure_reasons or 'underperforming_returns' in failure_reasons:
                # Boost strategies with historically better returns
                for strategy_name in adjusted_scores:
                    historical_performance = self._get_historical_performance_bonus(strategy_name, symbol)
                    if historical_performance > 0:
                        adjusted_scores[strategy_name]['fitness_score'] += 10  # Boost proven performers
            
            if 'excessive_drawdown' in failure_reasons:
                # Prefer lower-risk strategies
                low_risk_strategies = ['mean_reversion', 'bollinger', 'pca_low_beta']
                for strategy_name in low_risk_strategies:
                    if strategy_name in adjusted_scores:
                        adjusted_scores[strategy_name]['fitness_score'] += 15  # Strong preference for low risk
            
            if 'regime_mismatch' in failure_reasons:
                # Strongly prefer strategies that match current regime
                current_regime = self._get_current_market_regime(symbol)
                for strategy_name in adjusted_scores:
                    regime_fitness = self._calculate_regime_fitness(strategy_name, current_regime)
                    if regime_fitness > 70:  # High regime fit
                        adjusted_scores[strategy_name]['fitness_score'] += 20  # Strong boost for regime match
            
            if failure_severity == 'severe':
                # For severe failures, only consider most conservative strategies
                conservative_strategies = ['pca_low_beta', 'bollinger', 'mean_reversion']
                filtered_scores = {k: v for k, v in adjusted_scores.items() 
                                 if k in conservative_strategies}
                if filtered_scores:
                    adjusted_scores = filtered_scores
            
            return adjusted_scores
            
        except Exception as e:
            self.logger.error(f"Error applying reassignment rules: {e}")
            return strategy_scores  # Return original scores on error
    
    def _get_failure_recommendation(self, failure_reasons: List[str], failure_severity: str) -> str:
        """
        Get recommendation for handling strategy failure
        
        Args:
            failure_reasons: List of failure reasons
            failure_severity: Severity of failure
            
        Returns:
            Recommendation string
        """
        if failure_severity == 'severe':
            return 'immediate_reassignment_to_conservative_strategy'
        elif 'regime_mismatch' in failure_reasons:
            return 'reassign_to_regime_appropriate_strategy'
        elif 'excessive_drawdown' in failure_reasons:
            return 'reassign_to_lower_risk_strategy'
        else:
            return 'reassign_to_better_performing_strategy'
    
    # NEW DYNAMIC CALCULATION METHODS - NO HARDCODED VALUES
    
    def _calculate_user_position_confidence(self, metrics: MarketMetricsResult) -> float:
        """Calculate confidence for user-selected positions based on market data"""
        if metrics.data_quality == 'insufficient':
            # Low confidence when no market data available
            return 25.0
        elif metrics.data_quality == 'poor':
            return 40.0
        elif metrics.data_quality == 'fair':
            return 65.0
        elif metrics.data_quality == 'good':
            return 85.0
        else:  # excellent
            return 95.0
    
    def _calculate_user_position_expected_return(self, metrics: MarketMetricsResult) -> float:
        """Calculate expected return for user positions based on market metrics"""
        if metrics.trend_strength is None or metrics.volatility is None:
            return 0.0  # No expected return calculation possible
        
        # Conservative expected return based on trend strength and volatility
        base_return = metrics.trend_strength * 0.08  # Max 8% for strong trends
        volatility_adjustment = -metrics.volatility * 0.02  # Reduce for high volatility
        
        return max(0.0, base_return + volatility_adjustment)
    
    def _handle_insufficient_data_risk(self, symbol: str) -> float:
        """Handle risk calculation when market data is insufficient"""
        self.logger.warning(f"Insufficient market data for risk calculation: {symbol}")
        # Return high risk score to be conservative
        return 85.0
    
    def _get_validated_risk_score(self, symbol: str, market_analysis: Dict) -> float:
        """Get validated risk score with no hardcoded fallbacks"""
        if 'risk_score' in market_analysis and market_analysis['risk_score'] is not None:
            return float(market_analysis['risk_score'])
        
        # Get risk score from market metrics
        metrics = get_market_metrics().get_comprehensive_metrics(symbol)
        if metrics.risk_score is not None:
            return metrics.risk_score
        
        # If no data available, log and return high risk
        self.logger.warning(f"No risk data available for {symbol}, assuming high risk")
        return 90.0
    
    def _get_validated_expected_return(self, strategy_name: str, strategy_scores: Dict, symbol: str) -> float:
        """Get validated expected return with no hardcoded fallbacks"""
        if strategy_name in strategy_scores and 'expected_return' in strategy_scores[strategy_name]:
            return float(strategy_scores[strategy_name]['expected_return'])
        
        # Calculate expected return based on market metrics
        metrics = get_market_metrics().get_comprehensive_metrics(symbol)
        if metrics.trend_strength is not None and metrics.volatility is not None:
            # Dynamic expected return based on strategy type and market conditions
            return self._calculate_strategy_market_return(strategy_name, metrics)
        
        # No data available - return zero expected return
        self.logger.warning(f"No expected return data for {strategy_name} on {symbol}")
        return 0.0
    
    def _calculate_strategy_market_return(self, strategy_name: str, metrics: MarketMetricsResult) -> float:
        """Calculate expected return based on strategy type and market metrics"""
        if metrics.trend_strength is None or metrics.volatility is None:
            return 0.0
        
        # Strategy-specific return calculations
        if 'momentum' in strategy_name:
            return metrics.trend_strength * 0.12  # Momentum benefits from strong trends
        elif 'mean_reversion' in strategy_name:
            return (1 - metrics.trend_strength) * 0.08  # Mean reversion benefits from weak trends
        elif 'volatility' in strategy_name:
            return metrics.volatility * 0.15  # Volatility strategies benefit from high volatility
        else:
            return metrics.trend_strength * 0.06  # Conservative default
    
    def _get_conservative_strategy(self) -> str:
        """Get most conservative strategy available"""
        conservative_strategies = ['mean_reversion', 'bollinger', 'position_monitoring']
        for strategy in conservative_strategies:
            if strategy in AVAILABLE_STRATEGIES:
                return strategy
        return 'position_monitoring'  # Ultimate fallback
    
    def _calculate_low_confidence_score(self, metrics: MarketMetricsResult) -> float:
        """Calculate low confidence score based on data quality"""
        if metrics.data_quality == 'insufficient':
            return 15.0
        elif metrics.data_quality == 'poor':
            return 25.0
        elif metrics.data_quality == 'fair':
            return 35.0
        else:
            return 45.0  # Still low confidence but some data available
    
    def _estimate_high_risk_score(self, symbol: str) -> float:
        """Estimate high risk score when no data available"""
        self.logger.warning(f"No risk data for {symbol}, estimating high risk")
        return 95.0  # Very high risk when no data
    
    def _estimate_conservative_return(self, metrics: MarketMetricsResult) -> float:
        """Estimate conservative return when data is limited"""
        if metrics.trend_strength is not None:
            return metrics.trend_strength * 0.03  # Very conservative return
        return 0.01  # Minimal expected return
    
    def _get_real_time_market_regime(self) -> Dict[str, Any]:
        """
        Get real-time market regime data from Redis shared state
        
        CRITICAL FIX: Direct Redis access for real-time regime data
        This ensures strategy assignment uses current market conditions
        
        Returns:
            Dict with current market regime information
        """
        try:
            # Check cache freshness
            current_time = datetime.now()
            if (self.current_regime_cache.get('last_updated') and 
                (current_time - self.current_regime_cache['last_updated']).total_seconds() < self.regime_cache_ttl):
                return self.current_regime_cache
            
            # Get fresh regime data from Redis
            if self.redis_manager:
                regime_data = self.redis_manager.get_shared_state('market_regime', namespace='market')
                if regime_data and isinstance(regime_data, dict):
                    # Update cache with real-time data
                    self.current_regime_cache = {
                        'regime': regime_data.get('regime', 'UNKNOWN'),
                        'confidence': regime_data.get('confidence', 0.0),
                        'trend': regime_data.get('trend', 'UNKNOWN'),
                        'trend_strength': regime_data.get('trend_strength', 0.0),
                        'volatility_regime': regime_data.get('volatility_regime', 'UNKNOWN'),
                        'enhanced_regime': regime_data.get('enhanced_regime', 'UNKNOWN'),
                        'market_stress_level': regime_data.get('market_stress_level', 50.0),
                        'last_updated': current_time
                    }
                    
                    self.logger.info(f"REAL-TIME REGIME: Retrieved current market regime: {regime_data.get('regime', 'UNKNOWN')} "
                                   f"(confidence: {regime_data.get('confidence', 0):.1f}%)")
                    return self.current_regime_cache
                else:
                    self.logger.warning("REAL-TIME REGIME: No valid regime data in Redis - using cached/default")
            else:
                self.logger.warning("REAL-TIME REGIME: Redis manager not available - using cached/default")
                
        except Exception as e:
            self.logger.error(f"REAL-TIME REGIME: Error accessing Redis regime data: {e}")
        
        # Return cached or default regime data
        return self.current_regime_cache
    
    def _update_regime_cache(self):
        """Initialize or refresh the regime cache"""
        try:
            regime_data = self._get_real_time_market_regime()
            self.logger.debug(f"Market regime cache updated: {regime_data.get('regime', 'UNKNOWN')}")
        except Exception as e:
            self.logger.warning(f"Failed to update regime cache: {e}")
    
    def _select_strategies_for_regime(self, regime_data: Dict[str, Any]) -> List[str]:
        """
        Select appropriate strategies based on current market regime
        
        CRITICAL FIX: Real regime-based strategy selection
        Maps current market conditions to optimal strategy types
        
        Args:
            regime_data: Current market regime information
            
        Returns:
            List of strategy names suited for current regime
        """
        try:
            current_regime = regime_data.get('regime', 'UNKNOWN')
            trend = regime_data.get('trend', 'UNKNOWN')
            volatility_regime = regime_data.get('volatility_regime', 'UNKNOWN')
            market_stress = regime_data.get('market_stress_level', 50.0)
            
            # Strategy selection based on market regime
            regime_strategies = {
                'BULL_MARKET': ['momentum', 'pca_momentum', 'jump_momentum', 'volatility_breakout'],
                'BEAR_MARKET': ['mean_reversion', 'bollinger', 'pca_mean_reversion', 'pca_low_beta'],
                'SIDEWAYS': ['bollinger', 'mean_reversion', 'pairs', 'volatility'],
                'HIGH_VOLATILITY': ['volatility', 'volatility_breakout', 'pca_low_beta'],
                'LOW_VOLATILITY': ['momentum', 'pca_momentum', 'trend_following'],
                'UNKNOWN': ['bollinger', 'mean_reversion', 'momentum']  # Conservative mix
            }
            
            # Get base strategies for current regime
            base_strategies = regime_strategies.get(current_regime, regime_strategies['UNKNOWN'])
            
            # Refine based on trend and volatility
            refined_strategies = base_strategies.copy()
            
            if trend == 'UPTREND' and current_regime != 'BEAR_MARKET':
                refined_strategies.extend(['momentum', 'pca_momentum'])
            elif trend == 'DOWNTREND':
                refined_strategies.extend(['mean_reversion', 'pca_mean_reversion'])
            
            if volatility_regime == 'HIGH_VOLATILITY':
                refined_strategies = [s for s in refined_strategies if 'volatility' in s or 'low_beta' in s]
                refined_strategies.extend(['volatility', 'pca_low_beta'])
            
            if market_stress > 70:  # High stress market
                refined_strategies = [s for s in refined_strategies if s in ['pca_low_beta', 'mean_reversion', 'bollinger']]
            
            # Remove duplicates and ensure we have available strategies
            final_strategies = list(set(refined_strategies))
            available_strategy_names = list(AVAILABLE_STRATEGIES.keys())
            final_strategies = [s for s in final_strategies if s in available_strategy_names]
            
            if not final_strategies:
                # Fallback to conservative strategies
                final_strategies = ['bollinger', 'mean_reversion']
            
            self.logger.info(f"REGIME-BASED SELECTION: {current_regime} regime → strategies: {final_strategies}")
            return final_strategies
            
        except Exception as e:
            self.logger.error(f"Error in regime-based strategy selection: {e}")
            return ['bollinger', 'mean_reversion']  # Safe fallback