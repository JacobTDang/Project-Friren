"""
Assignment Models and Data Structures

Contains shared data structures for the 3-scenario assignment system.
Separated from strategy_assignment_engine.py to prevent circular imports.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime


class AssignmentReason(Enum):
    """Reasons for strategy assignment"""
    NEW_POSITION = "new_position"           # First assignment to new position
    REGIME_CHANGE = "regime_change"         # Market regime changed
    POOR_PERFORMANCE = "poor_performance"   # Current strategy underperforming
    DIVERSIFICATION = "diversification"     # Portfolio balance optimization
    MANUAL_OVERRIDE = "manual_override"     # User/system override
    REBALANCE = "rebalance"                # Periodic rebalancing
    USER_BUY_HOLD = "user_buy_hold"        # User manually selected stock for buy and hold
    DECISION_ENGINE_CHOICE = "decision_engine_choice"  # Decision engine algorithmic selection
    STRATEGY_REEVALUATION = "strategy_reevaluation"   # Position health monitor triggered reassignment


class AssignmentScenario(Enum):
    """3-Scenario Assignment System Scenarios"""
    USER_BUY_HOLD = "user_buy_hold"              # User manually creates buy-and-hold position
    DECISION_ENGINE_CHOICE = "decision_engine_choice"  # Decision engine algorithmic selection
    STRATEGY_REEVALUATION = "strategy_reevaluation"    # Position health monitor triggered reassignment


@dataclass
class StrategyAssignment:
    """
    Represents a strategy assignment decision with all necessary metadata.
    
    This dataclass contains the complete strategy assignment including confidence,
    risk assessment, expected returns, and assignment reasoning - all using
    dynamic market data calculations (NO hardcoded values).
    """
    
    # Core assignment details
    symbol: str
    recommended_strategy: str
    assignment_reason: AssignmentReason
    confidence_score: float  # 0-100, calculated from real market data
    
    # Market-based assessments (NO hardcoded fallbacks)
    risk_score: float        # Calculated from real market volatility and metrics
    expected_return: float   # Expected return based on market analysis
    position_size_factor: float = 1.0  # Position sizing multiplier
    
    # Assignment context and metadata
    assignment_timestamp: datetime = field(default_factory=datetime.now)
    assignment_scenario: Optional[AssignmentScenario] = None
    market_regime: Optional[str] = None
    
    # Performance and validation tracking
    performance_target: Optional[float] = None
    assignment_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Integration with decision engine (preserves existing patterns)
    requires_monitoring: bool = True
    priority_level: str = "normal"  # normal, high, urgent
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert assignment to dictionary for database storage"""
        return {
            'symbol': self.symbol,
            'recommended_strategy': self.recommended_strategy,
            'assignment_reason': self.assignment_reason.value,
            'confidence_score': self.confidence_score,
            'risk_score': self.risk_score,
            'expected_return': self.expected_return,
            'position_size_factor': self.position_size_factor,
            'assignment_timestamp': self.assignment_timestamp,
            'assignment_scenario': self.assignment_scenario.value if self.assignment_scenario else None,
            'market_regime': self.market_regime,
            'performance_target': self.performance_target,
            'assignment_metadata': self.assignment_metadata,
            'requires_monitoring': self.requires_monitoring,
            'priority_level': self.priority_level
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrategyAssignment':
        """Create assignment from dictionary (database restore)"""
        return cls(
            symbol=data['symbol'],
            recommended_strategy=data['recommended_strategy'],
            assignment_reason=AssignmentReason(data['assignment_reason']),
            confidence_score=data['confidence_score'],
            risk_score=data['risk_score'],
            expected_return=data['expected_return'],
            position_size_factor=data.get('position_size_factor', 1.0),
            assignment_timestamp=data.get('assignment_timestamp', datetime.now()),
            assignment_scenario=AssignmentScenario(data['assignment_scenario']) if data.get('assignment_scenario') else None,
            market_regime=data.get('market_regime'),
            performance_target=data.get('performance_target'),
            assignment_metadata=data.get('assignment_metadata', {}),
            requires_monitoring=data.get('requires_monitoring', True),
            priority_level=data.get('priority_level', 'normal')
        )


@dataclass
class ScenarioRequest:
    """
    Request for 3-scenario assignment coordination.
    
    Used by the ThreeScenarioCoordinator to route assignment requests
    to the appropriate scenario handler.
    """
    
    symbol: str
    scenario: AssignmentScenario
    
    # Scenario-specific data
    user_data: Optional[Dict[str, Any]] = None          # For user buy & hold scenario
    market_data: Optional[Dict[str, Any]] = None        # For decision engine scenario
    performance_data: Optional[Dict[str, Any]] = None   # For reevaluation scenario
    
    # Request metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    request_timestamp: datetime = field(default_factory=datetime.now)
    priority: str = "normal"
    
    def get_scenario_context(self) -> Dict[str, Any]:
        """Get context data for the specific scenario"""
        if self.scenario == AssignmentScenario.USER_BUY_HOLD:
            return self.user_data or {}
        elif self.scenario == AssignmentScenario.DECISION_ENGINE_CHOICE:
            return self.market_data or {}
        elif self.scenario == AssignmentScenario.STRATEGY_REEVALUATION:
            return self.performance_data or {}
        else:
            return {}


@dataclass
class AssignmentValidationResult:
    """
    Result of assignment validation to ensure no hardcoded values.
    
    Used by AssignmentValidator to verify all assignments use dynamic
    market data calculations.
    """
    
    is_valid: bool
    assignment: StrategyAssignment
    
    # Validation details
    confidence_source: str      # Where confidence score came from
    risk_source: str           # Where risk score came from
    return_source: str         # Where expected return came from
    
    # Hardcoded value detection
    hardcoded_values_detected: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    
    # Market data quality
    market_data_quality: str = "unknown"
    data_freshness_minutes: Optional[float] = None
    
    validation_timestamp: datetime = field(default_factory=datetime.now)
    
    def add_warning(self, warning: str):
        """Add validation warning"""
        self.validation_warnings.append(warning)
    
    def add_hardcoded_detection(self, field_name: str, value: Any):
        """Add hardcoded value detection"""
        self.hardcoded_values_detected.append(f"{field_name}={value}")
        self.is_valid = False


@dataclass
class ValidationResult:
    """
    Simple validation result for assignment validator.
    
    Provides the basic structure expected by the assignment validator
    for validation results.
    """
    
    is_valid: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    data_quality: str = "unknown"
    validation_timestamp: datetime = field(default_factory=datetime.now)