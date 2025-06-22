"""
Trading Engine Tools Module

This module provides access to all the core trading infrastructure components.
"""

# Core infrastructure components (always available)
from Friren_V1.trading_engine.portfolio_manager.tools.account_manager import AccountManager
from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager
from Friren_V1.trading_engine.portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface

# Execution and order management
from Friren_V1.trading_engine.portfolio_manager.tools.execution_engine import SimpleExecutionEngine
from Friren_V1.trading_engine.portfolio_manager.tools.order_manager import SimpleOrderManager

# Process and system management
from Friren_V1.trading_engine.portfolio_manager.tools.multiprocess_manager import MultiprocessManager
from Friren_V1.trading_engine.portfolio_manager.tools.system_health_monitor import SystemHealthTool

# Position sizing (basic math, no external dependencies)
from Friren_V1.trading_engine.portfolio_manager.tools.position_sizer import PurePositionSizer

# Core components that are always available
__all__ = [
    'AccountManager',
    'TradingDBManager',
    'SimpleAlpacaInterface',
    'SimpleExecutionEngine',
    'SimpleOrderManager',
    'MultiprocessManager',
    'SystemHealthTool',
    'PurePositionSizer'
]

# Optional components that may have external dependencies
try:
    from Friren_V1.trading_engine.portfolio_manager.tools.strategy_selector import StrategySelector
    __all__.append('StrategySelector')
except ImportError:
    pass

try:
    from Friren_V1.trading_engine.portfolio_manager.tools.watchlist_manager import WatchlistManager
    __all__.append('WatchlistManager')
except ImportError:
    pass
