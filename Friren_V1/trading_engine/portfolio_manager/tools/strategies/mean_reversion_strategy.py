import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools

class MeanReversionStrategy(BaseStrategy):
    """
    RSI Mean Reversion Strategy

    Extracted from rsi_bt.py - rsi_backtest.backtest_rsi_strategy()

    Logic:
    - BUY when RSI <= oversold_threshold (typically 30)
    - SELL when RSI >= overbought_threshold (typically 70)
    - Confidence based on how extreme RSI value is
    """

    def __init__(self, rsi_period: int = 14, oversold_threshold: float = 30,
                 overbought_threshold: float = 70):
        self.rsi_period = rsi_period
        self.oversold_threshold = oversold_threshold
        self.overbought_threshold = overbought_threshold
        self.data_tools = StockDataTools()
        super().__init__()

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """Generate mean reversion signal based on RSI levels"""

        # Validate required data
        if not self.validate_data(df, ['Close']):
            return StrategySignal('HOLD', 0, 'Insufficient data for RSI analysis')

        # Add RSI if not present
        df_with_rsi = self._ensure_rsi(df)

        if len(df_with_rsi) < self.rsi_period + 5:
            return StrategySignal('HOLD', 0, 'Insufficient data for RSI calculation')

        # Get current values
        current = df_with_rsi.iloc[-1]

        if pd.isna(current['RSI']):
            return StrategySignal('HOLD', 0, 'RSI not yet calculated')

        current_rsi = current['RSI']
        current_price = current['Close']

        # Calculate base signal
        base_signal, base_confidence = self._calculate_rsi_signal(current_rsi, df_with_rsi)

        # Apply sentiment filter (mean reversion specific)
        final_signal, final_confidence = self.apply_sentiment_filter(
            base_signal, base_confidence, sentiment
        )

        # Check confidence threshold
        if final_confidence < confidence_threshold:
            return StrategySignal(
                'HOLD',
                final_confidence,
                f'RSI signal below threshold: {final_confidence:.1f}% < {confidence_threshold}%'
            )

        # Generate reasoning
        reasoning = self._generate_reasoning(
            current_rsi, current_price, final_signal, final_confidence, sentiment
        )

        return StrategySignal(
            final_signal,
            final_confidence,
            reasoning,
            metadata={
                'rsi': current_rsi,
                'price': current_price,
                'oversold_threshold': self.oversold_threshold,
                'overbought_threshold': self.overbought_threshold,
                'sentiment_adjustment': final_confidence - base_confidence
            }
        )

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence based on RSI extremity"""

        if not self.validate_data(df, ['Close']):
            return 0.0

        df_with_rsi = self._ensure_rsi(df)

        if len(df_with_rsi) < self.rsi_period + 5:
            return 0.0

        current = df_with_rsi.iloc[-1]

        if pd.isna(current['RSI']):
            return 0.0

        current_rsi = current['RSI']

        # Calculate confidence based on how extreme RSI is
        if current_rsi <= self.oversold_threshold:
            # More oversold = higher confidence
            extremity = (self.oversold_threshold - current_rsi) / self.oversold_threshold
            confidence = min(100, 50 + extremity * 100)
        elif current_rsi >= self.overbought_threshold:
            # More overbought = higher confidence
            extremity = (current_rsi - self.overbought_threshold) / (100 - self.overbought_threshold)
            confidence = min(100, 50 + extremity * 100)
        else:
            # RSI in neutral zone - low confidence
            distance_from_neutral = abs(current_rsi - 50)
            confidence = max(0, 30 - distance_from_neutral)

        # Add trend context - mean reversion works better in ranging markets
        recent_data = df_with_rsi.tail(10)
        if len(recent_data) >= 5:
            price_range = recent_data['Close'].max() - recent_data['Close'].min()
            avg_price = recent_data['Close'].mean()
            volatility_factor = (price_range / avg_price) * 100

            # Moderate volatility is good for mean reversion
            if 2 <= volatility_factor <= 8:
                confidence *= 1.2
            elif volatility_factor > 15:  # Too volatile
                confidence *= 0.8

        return min(100, confidence)

    def _ensure_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has RSI indicator"""
        df_copy = df.copy()

        if 'RSI' not in df_copy.columns:
            df_copy = self.data_tools.add_rsi(df_copy, period=self.rsi_period)

        return df_copy

    def _calculate_rsi_signal(self, current_rsi: float, df_with_rsi: pd.DataFrame) -> tuple[str, float]:
        """Calculate base RSI signal and confidence"""

        base_signal = 'HOLD'
        base_confidence = 0.0

        # Oversold condition - potential buy
        if current_rsi <= self.oversold_threshold:
            base_signal = 'BUY'
            # Confidence increases as RSI gets more extreme
            extremity = (self.oversold_threshold - current_rsi) / self.oversold_threshold
            base_confidence = min(100, 60 + extremity * 40)

        # Overbought condition - potential sell
        elif current_rsi >= self.overbought_threshold:
            base_signal = 'SELL'
            # Confidence increases as RSI gets more extreme
            extremity = (current_rsi - self.overbought_threshold) / (100 - self.overbought_threshold)
            base_confidence = min(100, 60 + extremity * 40)

        # Add RSI divergence bonus if we can detect it
        if base_signal != 'HOLD' and len(df_with_rsi) >= 20:
            divergence_bonus = self._check_rsi_divergence(df_with_rsi, base_signal)
            base_confidence = min(100, base_confidence + divergence_bonus)

        return base_signal, base_confidence

    def _check_rsi_divergence(self, df_with_rsi: pd.DataFrame, signal: str) -> float:
        """Check for RSI divergence to add confidence"""
        recent_data = df_with_rsi.tail(20)

        if len(recent_data) < 10:
            return 0.0

        price_trend = recent_data['Close'].iloc[-1] - recent_data['Close'].iloc[-10]
        rsi_trend = recent_data['RSI'].iloc[-1] - recent_data['RSI'].iloc[-10]

        # Bullish divergence: price making lower lows, RSI making higher lows
        if signal == 'BUY' and price_trend < 0 and rsi_trend > 0:
            return 15.0  # Bonus confidence for bullish divergence

        # Bearish divergence: price making higher highs, RSI making lower highs
        elif signal == 'SELL' and price_trend > 0 and rsi_trend < 0:
            return 15.0  # Bonus confidence for bearish divergence

        return 0.0

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> tuple[str, float]:
        """Apply mean reversion specific sentiment filtering"""

        # Mean reversion often works AGAINST sentiment extremes
        adjusted_confidence = base_confidence

        if base_signal == 'BUY':
            # Buying oversold stocks works better when sentiment is negative
            if sentiment < -0.3:  # Negative sentiment supports oversold buy
                adjusted_confidence = min(100, adjusted_confidence * 1.25)
            elif sentiment > 0.3:  # Positive sentiment opposes oversold buy
                adjusted_confidence *= 0.8

        elif base_signal == 'SELL':
            # Selling overbought stocks works better when sentiment is positive
            if sentiment > 0.3:  # Positive sentiment supports overbought sell
                adjusted_confidence = min(100, adjusted_confidence * 1.25)
            elif sentiment < -0.3:  # Negative sentiment opposes overbought sell
                adjusted_confidence *= 0.8

        return base_signal, adjusted_confidence

    def _generate_reasoning(self, current_rsi: float, current_price: float,
                          signal: str, confidence: float, sentiment: float) -> str:
        """Generate human-readable reasoning for the signal"""

        if signal == 'BUY':
            reason = f"RSI oversold at {current_rsi:.1f} (< {self.oversold_threshold})"
            reason += f" suggests mean reversion opportunity"
        elif signal == 'SELL':
            reason = f"RSI overbought at {current_rsi:.1f} (> {self.overbought_threshold})"
            reason += f" suggests mean reversion opportunity"
        else:
            reason = f"RSI neutral at {current_rsi:.1f} - no mean reversion signal"

        # Add sentiment context for mean reversion
        if signal == 'BUY' and sentiment < -0.3:
            reason += f", negative sentiment supports oversold condition"
        elif signal == 'SELL' and sentiment > 0.3:
            reason += f", positive sentiment supports overbought condition"
        elif signal != 'HOLD' and abs(sentiment) > 0.3:
            sentiment_desc = "positive" if sentiment > 0 else "negative"
            reason += f", {sentiment_desc} sentiment may reduce mean reversion probability"

        return reason

    def get_required_indicators(self) -> list[str]:
        """Return required technical indicators"""
        return ['RSI']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""
        return StrategyMetadata(
            name="RSI Mean Reversion",
            category="MEAN_REVERSION",
            typical_holding_days=5,
            works_best_in="Range-bound markets with clear support/resistance levels",
            min_confidence=70.0,
            max_positions=1,
            requires_pairs=False
        )
