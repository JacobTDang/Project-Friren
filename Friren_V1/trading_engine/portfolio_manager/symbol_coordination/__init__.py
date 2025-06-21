"""
Symbol Coordination Module

This module provides symbol-specific coordination functionality for the
enhanced multi-symbol trading orchestrator. It enables per-symbol monitoring
intensity, resource allocation, and decision routing while maintaining the
5-process architecture constraint.

Key Components:
- SymbolCoordinator: Main coordination logic
- SymbolConfig: Per-symbol configuration classes
- ResourceManager: Resource allocation and API rate limiting
- MessageRouter: Symbol-specific message routing
- IntensityManager: Dynamic monitoring intensity management
"""

from .symbol_config import (
    SymbolMonitoringConfig,
    SymbolState,
    SymbolResourceBudget
)

from .symbol_coordinator import SymbolCoordinator

__all__ = [
    'SymbolMonitoringConfig',
    'SymbolState',
    'SymbolResourceBudget',
    'SymbolCoordinator'
]
