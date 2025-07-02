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
# random import removed - using real data only

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