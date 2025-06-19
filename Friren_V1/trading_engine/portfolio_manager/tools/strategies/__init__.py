"""
Pure Trading Strategies Package

This package contains ONLY strategy implementations - no orchestration logic.
The StrategySelector belongs in the portfolio manager layer.

Strategy Categories:
- MOMENTUM: Trend-following strategies
- MEAN_REVERSION: Mean reversion strategies
- VOLATILITY: Volatility-based strategies
- PAIRS: Pairs trading strategies
- FACTOR: Factor-based strategies

Each strategy implements BaseStrategy interface for signal generation only.
"""

from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from .momentum_strategy import MomentumStrategy
from .mean_reversion_strategy import MeanReversionStrategy
from .bollinger_strategy import BollingerStrategy
from .volatility_strategy import VolatilityStrategy
from .pairs_strategy import PairsStrategy
from .pca_strategy import PCAStrategy
from .jump_diffusion_strategy import JumpDiffusionStrategy

# Version info
__version__ = "1.0.0"
__author__ = "Jacob Dang"

# Strategy registry for easy discovery
AVAILABLE_STRATEGIES = {
    # Basic strategies
    'momentum': MomentumStrategy,
    'mean_reversion': MeanReversionStrategy,
    'bollinger': BollingerStrategy,
    'volatility': VolatilityStrategy,
    'pairs': PairsStrategy,

    # Advanced strategies with variants
    'pca_momentum': lambda: PCAStrategy(strategy_type='factor_momentum'),
    'pca_mean_reversion': lambda: PCAStrategy(strategy_type='factor_mean_reversion'),
    'pca_low_beta': lambda: PCAStrategy(strategy_type='low_beta'),
    'pca_idiosyncratic': lambda: PCAStrategy(strategy_type='idiosyncratic'),

    'jump_momentum': lambda: JumpDiffusionStrategy(strategy_type='jump_momentum'),
    'jump_reversal': lambda: JumpDiffusionStrategy(strategy_type='jump_reversal'),
    'volatility_breakout': lambda: JumpDiffusionStrategy(strategy_type='volatility_breakout'),
}

# Strategy metadata for discovery
STRATEGY_CATEGORIES = {
    'MOMENTUM': ['momentum', 'pca_momentum', 'jump_momentum'],
    'MEAN_REVERSION': ['mean_reversion', 'bollinger', 'pca_mean_reversion', 'jump_reversal'],
    'VOLATILITY': ['volatility', 'volatility_breakout'],
    'PAIRS': ['pairs'],
    'FACTOR': ['pca_momentum', 'pca_mean_reversion', 'pca_low_beta', 'pca_idiosyncratic'],
    'DEFENSIVE': ['pca_low_beta']
}

def get_available_strategies() -> list[str]:
    """Get list of all available strategy names"""
    return list(AVAILABLE_STRATEGIES.keys())

def get_strategies_by_category(category: str) -> list[str]:
    """Get strategies in a specific category"""
    return STRATEGY_CATEGORIES.get(category.upper(), [])

def create_strategy(strategy_type: str, **kwargs):
    """
    Create a strategy instance by type

    Args:
        strategy_type: Strategy identifier
        **kwargs: Strategy-specific parameters

    Returns:
        Strategy instance or None if type not found
    """
    if strategy_type not in AVAILABLE_STRATEGIES:
        raise ValueError(f"Unknown strategy: {strategy_type}. Available: {list(AVAILABLE_STRATEGIES.keys())}")

    strategy_factory = AVAILABLE_STRATEGIES[strategy_type]

    if callable(strategy_factory):
        return strategy_factory() if not kwargs else strategy_factory(**kwargs)
    else:
        return strategy_factory(**kwargs)

def discover_all_strategies() -> dict:
    """
    Discover and instantiate all available strategies

    Returns:
        Dict mapping strategy names to instances
    """
    strategies = {}

    for name, factory in AVAILABLE_STRATEGIES.items():
        try:
            if callable(factory):
                instance = factory()
            else:
                instance = factory()
            strategies[name] = instance
        except Exception as e:
            print(f"Warning: Could not create strategy {name}: {e}")

    return strategies

# Export main classes and functions (NO ORCHESTRATION)
__all__ = [
    # Base classes
    'BaseStrategy',
    'StrategySignal',
    'StrategyMetadata',

    # Strategy implementations
    'MomentumStrategy',
    'MeanReversionStrategy',
    'BollingerStrategy',
    'VolatilityStrategy',
    'PairsStrategy',
    'PCAStrategy',
    'JumpDiffusionStrategy',

    # Utilities for discovery (NOT orchestration)
    'AVAILABLE_STRATEGIES',
    'STRATEGY_CATEGORIES',
    'get_available_strategies',
    'get_strategies_by_category',
    'create_strategy',
    'discover_all_strategies'
]
