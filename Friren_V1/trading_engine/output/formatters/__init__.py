"""
Standardized Output Formatters for Friren Trading System
========================================================

This module contains all specialized formatters for different types of
trading intelligence output, ensuring consistent formatting across all
system components.

All formatters follow the zero-hardcoding principle - using only real
market data and dynamic calculations.
"""

from .news_formatter import NewsFormatter
from .analysis_formatter import AnalysisFormatter
from .trading_formatter import TradingFormatter
from .monitoring_formatter import MonitoringFormatter

__all__ = [
    'NewsFormatter',
    'AnalysisFormatter',
    'TradingFormatter', 
    'MonitoringFormatter'
]