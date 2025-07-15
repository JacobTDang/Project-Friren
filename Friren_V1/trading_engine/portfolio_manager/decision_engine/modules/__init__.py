"""
Enhanced Decision Engine Modules Package
========================================

Modular components for the enhanced decision engine, refactored for
better maintainability and separation of concerns.

Modules:
--------
- signal_processing_module: Signal aggregation, conflict resolution, message processing
- risk_management_module: Risk assessment, rate limiting, parameter adaptation
- decision_orchestrator: Main decision engine coordination and workflow management

Usage:
------
```python
from .decision_orchestrator import DecisionOrchestratorModule
from .signal_processing_module import SignalProcessingModule
from .risk_management_module import RiskManagementModule
```
"""

from .signal_processing_module import (
    SignalProcessingModule,
    AggregatedSignal,
    TransitionSignal,
    SignalProcessingMetrics,
    DecisionType,
    SignalWeight,
    TransitionSignalType
)

from .risk_management_module import (
    RiskManagementModule,
    RiskAssessment,
    PositionSizingResult,
    RiskManagementMetrics,
    RateLimiter,
    RiskLevel
)

from .decision_orchestrator import (
    DecisionOrchestratorModule,
    MarketDecisionEngine,  # Backward compatibility alias
    DecisionMetrics,
    MarketRegimeState,
    MonitoringStrategyStatus
)

__all__ = [
    # Main orchestrator
    'DecisionOrchestratorModule',
    'MarketDecisionEngine',
    
    # Individual modules
    'SignalProcessingModule',
    'RiskManagementModule',
    
    # Signal processing components
    'AggregatedSignal',
    'TransitionSignal',
    'SignalProcessingMetrics',
    'DecisionType',
    'SignalWeight',
    'TransitionSignalType',
    
    # Risk management components
    'RiskAssessment',
    'PositionSizingResult', 
    'RiskManagementMetrics',
    'RateLimiter',
    'RiskLevel',
    
    # Orchestrator components
    'DecisionMetrics',
    'MarketRegimeState',
    'MonitoringStrategyStatus'
]