"""
Three Scenario Strategy Assignment Coordinator

Coordinates strategy assignments across the three primary scenarios:
1. User Buy & Hold positions (manual overrides)
2. Decision Engine algorithmic choices (automated selection)
3. Position Health Monitor reassignments (performance-based)

This replaces hardcoded assignment logic with dynamic routing based on the
specific scenario context and integrates with the database migration 0005.
"""

import logging
from typing import Dict, Any, Optional, Union
from datetime import datetime

# Import shared models
from .models import AssignmentScenario, ScenarioRequest, StrategyAssignment

# Avoid circular import by importing inside methods when needed


class ThreeScenarioCoordinator:
    """
    Coordinates strategy assignments across three scenarios with dynamic data integration.
    
    NO HARDCODED VALUES - All assignments use market_metrics and real data calculations.
    """
    
    def __init__(self, assignment_engine=None):
        self.logger = logging.getLogger(__name__)
        # Use provided assignment engine to prevent circular initialization
        self.assignment_engine = assignment_engine
        
        # Scenario routing mapping
        self.scenario_handlers = {
            AssignmentScenario.USER_BUY_HOLD: self._handle_user_buy_hold,
            AssignmentScenario.DECISION_ENGINE_CHOICE: self._handle_decision_engine_choice,
            AssignmentScenario.STRATEGY_REEVALUATION: self._handle_strategy_reevaluation
        }
        
        self.logger.info("ThreeScenarioCoordinator initialized - dynamic assignment routing active")
    
    def route_assignment_request(self, request: ScenarioRequest):
        """Route assignment request to appropriate scenario handler"""
        
        try:
            self.logger.info(f"Routing assignment for {request.symbol} - scenario: {request.scenario.value}")
            
            # Get scenario-specific handler
            handler = self.scenario_handlers.get(request.scenario)
            if not handler:
                raise ValueError(f"Unknown assignment scenario: {request.scenario}")
            
            # Execute scenario-specific assignment
            assignment = handler(request)
            
            self.logger.info(f"Assignment completed for {request.symbol}: {assignment.recommended_strategy} "
                           f"(confidence: {assignment.confidence_score:.1f}%)")
            
            return assignment
            
        except Exception as e:
            self.logger.error(f"Assignment routing failed for {request.symbol}: {e}")
            raise
    
    def _handle_user_buy_hold(self, request: ScenarioRequest):
        """Handle user buy & hold position assignments"""
        
        self.logger.debug(f"Processing user buy & hold assignment for {request.symbol}")
        
        # Use existing assignment engine or raise error if not provided
        if not self.assignment_engine:
            raise RuntimeError("ThreeScenarioCoordinator requires assignment_engine to be provided during initialization to prevent circular dependencies")
        
        # User buy & hold always uses buy_and_hold strategy with user-specified confidence
        return self.assignment_engine.assign_strategy_for_user_position(
            symbol=request.symbol,
            user_intent="buy_hold"
        )
    
    def _handle_decision_engine_choice(self, request: ScenarioRequest):
        """Handle decision engine algorithmic strategy choices"""
        
        self.logger.debug(f"Processing decision engine choice for {request.symbol}")
        
        # Use existing assignment engine or raise error if not provided
        if not self.assignment_engine:
            raise RuntimeError("ThreeScenarioCoordinator requires assignment_engine to be provided during initialization to prevent circular dependencies")
        
        # Decision engine uses full algorithmic selection - call legacy method to avoid recursion
        return self.assignment_engine._legacy_assign_strategy_for_decision_engine(
            symbol=request.symbol,
            market_analysis=request.market_data or {},
            decision_context=request.metadata or {}
        )
    
    def _handle_strategy_reevaluation(self, request: ScenarioRequest):
        """Handle position health monitor strategy reassignments"""
        
        self.logger.debug(f"Processing strategy reevaluation for {request.symbol}")
        
        # Use existing assignment engine or raise error if not provided
        if not self.assignment_engine:
            raise RuntimeError("ThreeScenarioCoordinator requires assignment_engine to be provided during initialization to prevent circular dependencies")
        
        # Health monitor triggers performance-based reassignment
        return self.assignment_engine.reassign_strategy_from_health_monitor(
            symbol=request.symbol,
            current_strategy=request.metadata.get('current_strategy', 'unknown'),
            performance_data=request.performance_data or {},
            health_analysis=request.metadata or {}
        )


# Convenience functions for common usage patterns
def assign_user_buy_hold(symbol: str, user_confidence: Optional[float] = None, 
                        user_metadata: Optional[Dict] = None) -> StrategyAssignment:
    """Convenience function for user buy & hold assignments"""
    coordinator = ThreeScenarioCoordinator()
    request = ScenarioRequest(
        symbol=symbol,
        scenario=AssignmentScenario.USER_BUY_HOLD,
        override_confidence=user_confidence,
        user_data=user_metadata
    )
    return coordinator.route_assignment_request(request)


def assign_from_decision_engine(symbol: str, market_data: Optional[Dict] = None,
                              context: Optional[Dict] = None) -> StrategyAssignment:
    """Convenience function for decision engine assignments"""
    coordinator = ThreeScenarioCoordinator()
    request = ScenarioRequest(
        symbol=symbol,
        scenario=AssignmentScenario.DECISION_ENGINE_CHOICE,
        market_data=market_data,
        metadata=context
    )
    return coordinator.route_assignment_request(request)


def assign_from_health_monitor(symbol: str, performance_data: Optional[Dict] = None,
                             health_context: Optional[Dict] = None) -> StrategyAssignment:
    """Convenience function for health monitor reassignments"""
    coordinator = ThreeScenarioCoordinator()
    request = ScenarioRequest(
        symbol=symbol,
        scenario=AssignmentScenario.STRATEGY_REEVALUATION,
        performance_data=performance_data,
        metadata=health_context
    )
    return coordinator.route_assignment_request(request)