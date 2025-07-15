from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional, List, Any
import pandas as pd
import numpy as np

@dataclass
class StrategySignal:
    """Standardized signal output from strategies"""
    action: str  # 'BUY', 'SELL', 'HOLD'
    confidence: float  # 0-100 scale
    reasoning: str  # readable explanation
    metadata: Optional[Dict[str, Any]] = None  # Additional strategy-specific data

    def __post_init__(self):
        # Validate action
        if self.action not in ['BUY', 'SELL', 'HOLD']:
            raise ValueError(f"Invalid action: {self.action}")

        # Clamp confidence to 0-100
        self.confidence = max(0, min(100, self.confidence))

@dataclass
class StrategyMetadata:
    """Metadata about strategy characteristics"""
    name: str
    category: str  # 'MOMENTUM', 'MEAN_REVERSION', 'VOLATILITY', 'PAIRS'
    typical_holding_days: int
    works_best_in: str
    min_confidence: float
    max_positions: int = 1
    requires_pairs: bool = False

class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies

    Each strategy should extract core signal generation logic from backtest classes
    without portfolio management, performance tracking, or execution details.
    """

    def __init__(self):
        self.metadata = self._get_strategy_metadata()

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """
        Generate trading signal based on current market data

        Args:
            df: OHLCV DataFrame with technical indicators added
            sentiment: Market sentiment score (-1.0 to +1.0)
            confidence_threshold: Minimum confidence required for signal (0-100)

        Returns:
            StrategySignal with action, confidence, and reasoning
        """
        pass

    @abstractmethod
    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """
        Calculate confidence score for current market conditions

        Args:
            df: OHLCV DataFrame with technical indicators

        Returns:
            Confidence score (0-100)
        """
        pass

    @abstractmethod
    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata for orchestrator"""
        pass

    def validate_data(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that DataFrame has required data for strategy

        Args:
            df: DataFrame to validate
            required_columns: List of required column names

        Returns:
            True if data is valid
        """
        # FIXED: Handle both DataFrame and dict data types safely
        if hasattr(df, 'empty') and df.empty:
            return False
        elif isinstance(df, dict) and (not df or len(df) == 0):
            return False
        elif df is None:
            return False

        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"Missing required columns for {self.metadata.name}: {missing_cols}")
            return False

        # Check for sufficient data
        if len(df) < 30:  # Minimum 30 periods
            print(f"Insufficient data for {self.metadata.name}: {len(df)} rows")
            return False

        return True

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> tuple[str, float]:
        """
        Apply sentiment filtering to base signal

        Args:
            base_signal: Original signal ('BUY', 'SELL', 'HOLD')
            base_confidence: Original confidence (0-100)
            sentiment: Market sentiment (-1.0 to +1.0)

        Returns:
            Tuple of (filtered_signal, adjusted_confidence)
        """
        # Default implementation - can be overridden by specific strategies
        adjusted_confidence = base_confidence

        # Reduce confidence if sentiment strongly opposes signal
        if base_signal == 'BUY' and sentiment < -0.5:
            adjusted_confidence *= 0.7  # Reduce by 30%
        elif base_signal == 'SELL' and sentiment > 0.5:
            adjusted_confidence *= 0.7

        # Boost confidence if sentiment supports signal
        elif base_signal == 'BUY' and sentiment > 0.3:
            adjusted_confidence = min(100, adjusted_confidence * 1.2)
        elif base_signal == 'SELL' and sentiment < -0.3:
            adjusted_confidence = min(100, adjusted_confidence * 1.2)

        return base_signal, adjusted_confidence

    def get_required_indicators(self) -> List[str]:
        """
        Return list of technical indicators required by this strategy
        Default implementation returns common indicators
        """
        return ['SMA_10', 'SMA_20', 'RSI', 'Volume']

    def __str__(self):
        return f"{self.metadata.name} ({self.metadata.category})"

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.metadata.name}>"
