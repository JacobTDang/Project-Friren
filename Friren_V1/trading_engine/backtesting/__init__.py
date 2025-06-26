# PRODUCTION: Suppress backtest import warnings - these are optional components
import logging
logging.getLogger().setLevel(logging.ERROR)  # Suppress warnings during import

try:
    from .ma_bt import MA_backtester
except ImportError:
    MA_backtester = None

try:
    from .rsi_bt import rsi_backtest
except ImportError:
    rsi_backtest = None

try:
    from .bolinger_bt import bb_backtest
except ImportError:
    bb_backtest = None

try:
    from .kalman_bt import KalmanMeanReversionBacktest
except ImportError:
    KalmanMeanReversionBacktest = None

try:
    from .conPair_bt import CointegrationPairsBacktest
except ImportError:
    CointegrationPairsBacktest = None

__all__ = []
for item in [MA_backtester, rsi_backtest, bb_backtest,
             KalmanMeanReversionBacktest, CointegrationPairsBacktest]:
    if item is not None:
        __all__.append(item.__name__)
