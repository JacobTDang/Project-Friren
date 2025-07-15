"""
3-Scenario Strategy Assignment Integration

This module provides the infrastructure for coordinating strategy assignments across
the three primary scenarios in the Friren trading system:

1. User Buy & Hold positions
2. Decision Engine algorithmic choices  
3. Position Health Monitor reassignments

All assignments are tracked in the database with full audit trails and integrate
seamlessly with the existing RedisBaseProcess communication infrastructure.
"""

from .scenario_coordinator import ThreeScenarioCoordinator
from .assignment_validator import AssignmentValidator
from .database_integrator import DatabaseIntegrator

__all__ = [
    'ThreeScenarioCoordinator',
    'AssignmentValidator', 
    'DatabaseIntegrator'
]