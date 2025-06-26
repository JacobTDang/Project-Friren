"""
trading_engine/portfolio_manager/decision_engine/__init__.py
"""

# Core components
try:
    from .decision_engine import EnhancedMarketDecisionEngineProcess as MarketDecisionEngineProcess
    from .signal_aggregator import SignalAggregator, AggregatedSignal
    from .conflict_resolver import ConflictResolver, ResolvedDecision
    from .risk_manager import SolidRiskManager, RiskValidationResult
    from .parameter_adapter import ParameterAdapter, AdaptationMetrics
    from .execution_orchestrator import ExecutionOrchestrator, ExecutionResult

    AVAILABLE = True

except ImportError as e:
    AVAILABLE = False
    print(f"Warning: Decision engine components not fully available: {e}")

# Simple exports
__all__ = [
    'MarketDecisionEngineProcess',
    'SignalAggregator', 'AggregatedSignal',
    'ConflictResolver', 'ResolvedDecision',
    'SolidRiskManager', 'RiskValidationResult',
    'ParameterAdapter', 'AdaptationMetrics',
    'ExecutionOrchestrator', 'ExecutionResult',
    'AVAILABLE'
]

__version__ = "1.0.0"
