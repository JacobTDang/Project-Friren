"""
Database Integration for 3-Scenario Strategy Assignments

Integrates strategy assignments with the database migration 0005 fields and provides
atomic updates with full audit trails for the three assignment scenarios.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

from .models import StrategyAssignment, AssignmentReason, AssignmentScenario


@dataclass 
class DatabaseAssignmentRecord:
    """Database record for strategy assignment tracking"""
    symbol: str
    current_strategy: str
    assignment_scenario: str
    assignment_reason: str
    strategy_confidence: float
    strategy_assigned_at: datetime
    previous_strategy: Optional[str] = None
    assignment_metadata: Optional[Dict[str, Any]] = None


class DatabaseIntegrator:
    """
    Integrates strategy assignments with database using migration 0005 fields.
    
    Provides atomic updates and complete audit trails for all three assignment scenarios.
    """
    
    def __init__(self, db_manager=None):
        self.logger = logging.getLogger(__name__)
        self.db_manager = db_manager
        
        # Scenario to database field mappings
        self.scenario_mappings = {
            AssignmentScenario.USER_BUY_HOLD: 'user_buy_hold',
            AssignmentScenario.DECISION_ENGINE_CHOICE: 'decision_engine_choice', 
            AssignmentScenario.STRATEGY_REEVALUATION: 'strategy_reevaluation'
        }
        
        # Assignment reason mappings
        self.reason_mappings = {
            AssignmentReason.NEW_POSITION: 'new_position',
            AssignmentReason.REGIME_CHANGE: 'regime_change',
            AssignmentReason.POOR_PERFORMANCE: 'poor_performance',
            AssignmentReason.DIVERSIFICATION: 'diversification',
            AssignmentReason.MANUAL_OVERRIDE: 'manual_override',
            AssignmentReason.REBALANCE: 'rebalance',
            AssignmentReason.USER_BUY_HOLD: 'user_buy_hold',
            AssignmentReason.DECISION_ENGINE_CHOICE: 'decision_engine_choice',
            AssignmentReason.STRATEGY_REEVALUATION: 'strategy_reevaluation'
        }
        
        self.logger.info("DatabaseIntegrator initialized - migration 0005 field integration active")
    
    def record_assignment(self, assignment: StrategyAssignment, 
                         scenario: AssignmentScenario) -> bool:
        """Record strategy assignment in database with full audit trail"""
        
        try:
            # Create database record
            db_record = self._create_database_record(assignment, scenario)
            
            # Apply assignment to database
            success = self._apply_assignment_to_database(db_record)
            
            if success:
                self.logger.info(f"Database assignment recorded for {assignment.symbol}: "
                               f"{assignment.recommended_strategy} via {scenario.value}")
            else:
                self.logger.error(f"Failed to record assignment for {assignment.symbol}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Database assignment recording failed for {assignment.symbol}: {e}")
            return False
    
    def get_assignment_history(self, symbol: str, limit: int = 10) -> List[DatabaseAssignmentRecord]:
        """Retrieve assignment history for a symbol"""
        
        try:
            if not self.db_manager:
                self.logger.warning("No database manager available for history retrieval")
                return []
            
            # Query assignment history from database
            history_data = self.db_manager.get_assignment_history(symbol, limit)
            
            # Convert to DatabaseAssignmentRecord objects
            history_records = []
            for record in history_data:
                history_records.append(DatabaseAssignmentRecord(
                    symbol=record['symbol'],
                    current_strategy=record['current_strategy'],
                    assignment_scenario=record['assignment_scenario'],
                    assignment_reason=record['assignment_reason'],
                    strategy_confidence=float(record['strategy_confidence']),
                    strategy_assigned_at=record['strategy_assigned_at'],
                    previous_strategy=record.get('previous_strategy'),
                    assignment_metadata=record.get('assignment_metadata')
                ))
            
            self.logger.debug(f"Retrieved {len(history_records)} assignment records for {symbol}")
            return history_records
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve assignment history for {symbol}: {e}")
            return []
    
    def get_current_assignments(self, scenario: Optional[AssignmentScenario] = None) -> Dict[str, DatabaseAssignmentRecord]:
        """Get current strategy assignments, optionally filtered by scenario"""
        
        try:
            if not self.db_manager:
                self.logger.warning("No database manager available for current assignments")
                return {}
            
            # Build query filters
            filters = {}
            if scenario:
                filters['assignment_scenario'] = self.scenario_mappings[scenario]
            
            # Query current assignments
            current_data = self.db_manager.get_current_assignments(filters)
            
            # Convert to DatabaseAssignmentRecord objects
            current_assignments = {}
            for record in current_data:
                symbol = record['symbol']
                current_assignments[symbol] = DatabaseAssignmentRecord(
                    symbol=symbol,
                    current_strategy=record['current_strategy'],
                    assignment_scenario=record['assignment_scenario'],
                    assignment_reason=record['assignment_reason'],
                    strategy_confidence=float(record['strategy_confidence']),
                    strategy_assigned_at=record['strategy_assigned_at'],
                    previous_strategy=record.get('previous_strategy'),
                    assignment_metadata=record.get('assignment_metadata')
                )
            
            self.logger.debug(f"Retrieved {len(current_assignments)} current assignments"
                            f"{' for scenario ' + scenario.value if scenario else ''}")
            return current_assignments
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve current assignments: {e}")
            return {}
    
    def _create_database_record(self, assignment: StrategyAssignment, 
                              scenario: AssignmentScenario) -> DatabaseAssignmentRecord:
        """Create database record from strategy assignment"""
        
        return DatabaseAssignmentRecord(
            symbol=assignment.symbol,
            current_strategy=assignment.recommended_strategy,
            assignment_scenario=self.scenario_mappings[scenario],
            assignment_reason=self.reason_mappings.get(assignment.assignment_reason, 'unknown'),
            strategy_confidence=assignment.confidence_score,
            strategy_assigned_at=datetime.now(),
            previous_strategy=assignment.assignment_metadata.get('previous_strategy'),
            assignment_metadata={
                'market_regime': assignment.market_regime,
                'risk_score': assignment.risk_score,
                'expected_return': assignment.expected_return,
                'reasoning': assignment.assignment_metadata.get('reasoning', ''),
                'assignment_metadata': assignment.assignment_metadata
            }
        )
    
    def _apply_assignment_to_database(self, record: DatabaseAssignmentRecord) -> bool:
        """Apply assignment record to database using assign_strategy method"""
        
        try:
            if not self.db_manager:
                self.logger.warning("No database manager available - assignment not persisted")
                return False
            
            # Use the assign_strategy method from the CurrentHoldings model
            success = self.db_manager.assign_strategy_to_holding(
                symbol=record.symbol,
                strategy_name=record.current_strategy,
                scenario=record.assignment_scenario,
                reason=record.assignment_reason,
                confidence=record.strategy_confidence,
                metadata=record.assignment_metadata
            )
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to apply assignment to database: {e}")
            return False
    
    def validate_database_schema(self) -> bool:
        """Validate that migration 0005 fields are available"""
        
        try:
            if not self.db_manager:
                self.logger.warning("No database manager available for schema validation")
                return False
            
            # Check for migration 0005 fields
            required_fields = [
                'current_strategy',
                'assignment_scenario', 
                'assignment_reason',
                'strategy_confidence',
                'strategy_assigned_at',
                'previous_strategy',
                'assignment_metadata'
            ]
            
            schema_valid = self.db_manager.validate_assignment_fields(required_fields)
            
            if schema_valid:
                self.logger.info("Database schema validation passed - migration 0005 fields available")
            else:
                self.logger.error("Database schema validation failed - migration 0005 fields missing")
            
            return schema_valid
            
        except Exception as e:
            self.logger.error(f"Database schema validation failed: {e}")
            return False


# Convenience functions for common database operations
def record_user_assignment(assignment: StrategyAssignment, db_manager=None) -> bool:
    """Record user buy & hold assignment to database"""
    integrator = DatabaseIntegrator(db_manager)
    return integrator.record_assignment(assignment, AssignmentScenario.USER_BUY_HOLD)


def record_decision_engine_assignment(assignment: StrategyAssignment, db_manager=None) -> bool:
    """Record decision engine assignment to database"""
    integrator = DatabaseIntegrator(db_manager)
    return integrator.record_assignment(assignment, AssignmentScenario.DECISION_ENGINE_CHOICE)


def record_health_monitor_assignment(assignment: StrategyAssignment, db_manager=None) -> bool:
    """Record health monitor reassignment to database"""
    integrator = DatabaseIntegrator(db_manager)
    return integrator.record_assignment(assignment, AssignmentScenario.STRATEGY_REEVALUATION)