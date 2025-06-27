# LAZY LOADING: Import only when needed to avoid MemoryError during startup
# Heavy dependencies (yfinance, pandas) will only load when actually used

def _lazy_import_stock_data_tools():
    """Lazy import StockDataTools to avoid memory issues during startup"""
    try:
        from .data.data_utils import StockDataTools
        return StockDataTools
    except ImportError:
        return None

def _lazy_import_yahoo_price_data():
    """Lazy import YahooFinancePriceData to avoid memory issues during startup"""
    try:
        from .data.yahoo_price import YahooFinancePriceData
        return YahooFinancePriceData
    except ImportError:
        return None

def _lazy_import_bb_backtest():
    """Lazy import bollinger backtest to avoid memory issues during startup"""
    try:
        from .backtesting.bolinger_bt import bb_backtest
        return bb_backtest
    except ImportError:
        return None

# Module-level getattr for lazy loading
def __getattr__(name: str):
    if name == 'StockDataTools':
        result = _lazy_import_stock_data_tools()
        if result is None:
            raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
        return result
    elif name == 'YahooFinancePriceData':
        result = _lazy_import_yahoo_price_data()
        if result is None:
            raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
        return result
    elif name == 'bb_backtest':
        result = _lazy_import_bb_backtest()
        if result is None:
            raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
        return result
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# List available components (they will be imported when accessed)
__all__ = ['StockDataTools', 'YahooFinancePriceData', 'bb_backtest']
