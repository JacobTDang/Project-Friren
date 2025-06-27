#!/usr/bin/env python3
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
import random

from .strategies import AVAILABLE_STRATEGIES
from .strategy_selector import StrategySelector
from ..analytics.position_health_analyzer import ActiveStrategy, StrategyStatus

class AssignmentReason(Enum):
    """Reasons for strategy assignment"""
    NEW_POSITION = "new_position"           # First assignment to new position
    REGIME_CHANGE = "regime_change"         # Market regime changed
    POOR_PERFORMANCE = "poor_performance"   # Current strategy underperforming
    DIVERSIFICATION = "diversification"     # Portfolio balance optimization
    MANUAL_OVERRIDE = "manual_override"     # User/system override
    REBALANCE = "rebalance"                # Periodic rebalancing

@dataclass
class StrategyAssignment:
    """Result of strategy assignment analysis"""
    symbol: str
    recommended_strategy: str
    confidence_score: float  # 0-100
    reason: AssignmentReason
    market_regime: str
    risk_score: float
    expected_return: float
    assignment_time: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

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
    """
    
    def __init__(self, symbols: List[str], risk_tolerance: str = "moderate"):
        self.symbols = symbols
        self.risk_tolerance = risk_tolerance
        self.logger = logging.getLogger("strategy_assignment_engine")
        
        # Initialize strategy selector for market analysis
        self.strategy_selector = StrategySelector()
        
        # Track assignment history
        self.assignment_history: Dict[str, List[StrategyAssignment]] = {}
        self.current_assignments: Dict[str, StrategyAssignment] = {}
        
        # Performance tracking
        self.strategy_performance: Dict[str, Dict[str, float]] = {}
        
        # Assignment rules
        self.assignment_rules = self._create_assignment_rules()
        
        # Strategy categories for diversification
        self.strategy_categories = {
            'momentum': ['momentum', 'pca_momentum', 'jump_momentum'],
            'mean_reversion': ['mean_reversion', 'bollinger', 'pca_mean_reversion', 'jump_reversal'],
            'volatility': ['volatility', 'volatility_breakout'],
            'factor': ['pca_low_beta', 'pca_idiosyncratic'],
            'pairs': ['pairs']
        }
        
        self.logger.info(f"StrategyAssignmentEngine initialized for {len(symbols)} symbols")
        self.logger.info(f"Risk tolerance: {risk_tolerance}")
        self.logger.info(f"Available strategies: {len(AVAILABLE_STRATEGIES)}")
    
    def assign_strategy_to_position(self, symbol: str, current_strategy: Optional[str] = None,
                                  force_reason: Optional[AssignmentReason] = None) -> StrategyAssignment:
        """
        Assign optimal strategy to a specific position
        
        Args:
            symbol: Stock symbol to assign strategy for
            current_strategy: Currently assigned strategy (if any)
            force_reason: Force specific assignment reason
            
        Returns:
            StrategyAssignment with recommended strategy and metadata
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
                risk_score=market_analysis.get('risk_score', 50.0),
                expected_return=strategy_scores.get(best_strategy, {}).get('expected_return', 0.0),
                metadata={
                    'market_analysis': market_analysis,
                    'strategy_scores': strategy_scores,
                    'previous_strategy': current_strategy,
                    'alternatives': list(filtered_strategies.keys())[:3]
                }
            )
            
            # Step 7: Record assignment
            self._record_assignment(assignment)
            
            self.logger.info(f"✅ Strategy assigned to {symbol}: {best_strategy} ({confidence:.1f}% confidence)")
            self.logger.info(f"   Reason: {reason.value}, Market: {assignment.market_regime}")
            
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
            
            # Add symbol-specific analysis
            analysis = {
                'regime': market_data.get('primary_regime', 'UNKNOWN'),
                'volatility': random.uniform(0.15, 0.35),  # Mock volatility for now
                'trend_strength': random.uniform(0.3, 0.8),
                'risk_score': random.uniform(30, 70),
                'liquidity_score': random.uniform(80, 95),  # AAPL is highly liquid
                'sector': 'technology',  # Could be dynamic
                'market_cap': 'large_cap'
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
                    'expected_return': fitness_score * 0.001,  # Mock expected return
                    'risk_score': random.uniform(20, 80),
                    'complexity': self._get_strategy_complexity(strategy_name)
                }
                
            except Exception as e:
                self.logger.debug(f"Error scoring strategy {strategy_name}: {e}")
                strategy_scores[strategy_name] = {'fitness_score': 50.0, 'expected_return': 0.0}
        
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
        """Select the best strategy from scored options"""
        if not strategy_scores:
            return 'momentum', 60.0  # Safe default
        
        # Sort by fitness score
        sorted_strategies = sorted(strategy_scores.items(), 
                                 key=lambda x: x[1].get('fitness_score', 0), 
                                 reverse=True)
        
        best_strategy, best_data = sorted_strategies[0]
        confidence = best_data.get('fitness_score', 60.0)
        
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
    
    def _create_fallback_assignment(self, symbol: str, current_strategy: Optional[str]) -> StrategyAssignment:
        """Create safe fallback assignment"""
        return StrategyAssignment(
            symbol=symbol,
            recommended_strategy=current_strategy or 'momentum',
            confidence_score=60.0,
            reason=AssignmentReason.NEW_POSITION,
            market_regime='UNKNOWN',
            risk_score=50.0,
            expected_return=0.05,
            metadata={'fallback': True}
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
                    confidence_score=assignment.confidence_score * 0.9,  # Slightly lower confidence
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
        # Placeholder for historical performance lookup
        return random.uniform(-5, 10)  # -5 to +10 point bonus
    
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