import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools

class MomentumStrategy(BaseStrategy):
    """
    Moving Average Momentum Strategy

    Extracted from ma_bt.py - MA_backtester.backtest_ma_strategy()

    Logic:
    - BUY when SMA_10 > SMA_20 (golden cross)
    - SELL when SMA_10 < SMA_20 (death cross)
    - Confidence based on SMA separation magnitude
    """

    def __init__(self, short_ma: int = 10, long_ma: int = 20):
        self.short_ma = short_ma
        self.long_ma = long_ma
        self.data_tools = StockDataTools()
        super().__init__()

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """Generate momentum signal based on SMA crossover"""

        # Validate required data
        if not self.validate_data(df, ['Close']):
            return StrategySignal('HOLD', 0, 'Insufficient data for momentum analysis')

        # Add moving averages if not present
        df_with_ma = self._ensure_moving_averages(df)

        if len(df_with_ma) < self.long_ma + 5:
            return StrategySignal('HOLD', 0, 'Insufficient data for moving averages')

        # Get current values (latest row)
        current = df_with_ma.iloc[-1]

        # Check if we have valid MA values
        if pd.isna(current[f'SMA_{self.short_ma}']) or pd.isna(current[f'SMA_{self.long_ma}']):
            return StrategySignal('HOLD', 0, 'Moving averages not yet calculated')

        sma_short = current[f'SMA_{self.short_ma}']
        sma_long = current[f'SMA_{self.long_ma}']
        current_price = current['Close']

        # Calculate base signal
        base_signal, base_confidence = self._calculate_momentum_signal(
            sma_short, sma_long, current_price, df_with_ma
        )

        # Apply sentiment filter
        final_signal, final_confidence = self.apply_sentiment_filter(
            base_signal, base_confidence, sentiment
        )

        # Check confidence threshold
        if final_confidence < confidence_threshold:
            return StrategySignal(
                'HOLD',
                final_confidence,
                f'Momentum signal below threshold: {final_confidence:.1f}% < {confidence_threshold}%'
            )

        # Generate reasoning
        reasoning = self._generate_reasoning(
            sma_short, sma_long, current_price, final_signal, final_confidence, sentiment
        )

        return StrategySignal(
            final_signal,
            final_confidence,
            reasoning,
            metadata={
                'sma_short': sma_short,
                'sma_long': sma_long,
                'price': current_price,
                'sentiment_adjustment': final_confidence - base_confidence
            }
        )

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence based on SMA separation and trend strength"""

        if not self.validate_data(df, ['Close']):
            return 0.0

        df_with_ma = self._ensure_moving_averages(df)

        if len(df_with_ma) < self.long_ma + 5:
            return 0.0

        current = df_with_ma.iloc[-1]

        if pd.isna(current[f'SMA_{self.short_ma}']) or pd.isna(current[f'SMA_{self.long_ma}']):
            return 0.0

        sma_short = current[f'SMA_{self.short_ma}']
        sma_long = current[f'SMA_{self.long_ma}']

        # Calculate confidence based on SMA separation
        separation = abs(sma_short - sma_long) / sma_long
        confidence = min(100, separation * 1000)  # Scale to 0-100

        # Add trend consistency bonus
        recent_ma_diff = df_with_ma[f'SMA_{self.short_ma}'].iloc[-5:] - df_with_ma[f'SMA_{self.long_ma}'].iloc[-5:]
        trend_consistency = len(recent_ma_diff[recent_ma_diff * recent_ma_diff.iloc[-1] > 0]) / 5
        confidence *= (0.5 + 0.5 * trend_consistency)

        return min(100, confidence)

    def _ensure_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has required moving averages"""
        df_copy = df.copy()

        ma_list = [self.short_ma, self.long_ma]
        existing_mas = [int(col.split('_')[1]) for col in df_copy.columns if col.startswith('SMA_')]
        missing_mas = [ma for ma in ma_list if ma not in existing_mas]

        if missing_mas:
            df_copy = self.data_tools.add_moving_averages(
                df_copy,
                ma_list=missing_mas,
                ma_types=['SMA']
            )

        return df_copy

    def _calculate_momentum_signal(self, sma_short: float, sma_long: float,
                                 current_price: float, df_with_ma: pd.DataFrame) -> tuple[str, float]:
        """Calculate base momentum signal and confidence"""

        # Golden cross (bullish) or death cross (bearish)
        if sma_short > sma_long:
            base_signal = 'BUY'
        elif sma_short < sma_long:
            base_signal = 'SELL'
        else:
            base_signal = 'HOLD'

        # Calculate confidence based on:
        # 1. SMA separation magnitude
        separation = abs(sma_short - sma_long) / sma_long
        separation_confidence = min(100, separation * 1000)

        # 2. Price position relative to MAs
        price_above_short = current_price > sma_short
        price_above_long = current_price > sma_long

        if base_signal == 'BUY' and price_above_short and price_above_long:
            position_bonus = 20
        elif base_signal == 'SELL' and not price_above_short and not price_above_long:
            position_bonus = 20
        else:
            position_bonus = 0

        # 3. Recent crossover strength
        recent_data = df_with_ma.tail(10)
        if len(recent_data) >= 5:
            recent_short = recent_data[f'SMA_{self.short_ma}'].iloc[-1]
            recent_long = recent_data[f'SMA_{self.long_ma}'].iloc[-1]
            prev_short = recent_data[f'SMA_{self.short_ma}'].iloc[-5]
            prev_long = recent_data[f'SMA_{self.long_ma}'].iloc[-5]

            momentum_strength = abs((recent_short - recent_long) - (prev_short - prev_long)) / recent_long
            momentum_bonus = min(20, momentum_strength * 2000)
        else:
            momentum_bonus = 0

        base_confidence = min(100, separation_confidence + position_bonus + momentum_bonus)

        return base_signal, base_confidence

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> tuple[str, float]:
        """Apply momentum-specific sentiment filtering"""

        # Momentum strategies work better when aligned with sentiment
        adjusted_confidence = base_confidence

        if base_signal == 'BUY':
            if sentiment < -0.3:  # Negative sentiment opposes bullish momentum
                adjusted_confidence *= 0.6  # Reduce confidence significantly
            elif sentiment > 0.2:  # Positive sentiment supports bullish momentum
                adjusted_confidence = min(100, adjusted_confidence * 1.3)

        elif base_signal == 'SELL':
            if sentiment > 0.3:  # Positive sentiment opposes bearish momentum
                adjusted_confidence *= 0.6
            elif sentiment < -0.2:  # Negative sentiment supports bearish momentum
                adjusted_confidence = min(100, adjusted_confidence * 1.3)

        return base_signal, adjusted_confidence

    def _generate_reasoning(self, sma_short: float, sma_long: float, current_price: float,
                          signal: str, confidence: float, sentiment: float) -> str:
        """Generate human-readable reasoning for the signal"""

        separation_pct = abs(sma_short - sma_long) / sma_long * 100

        if signal == 'BUY':
            reason = f"Golden cross: SMA{self.short_ma} (${sma_short:.2f}) > SMA{self.long_ma} (${sma_long:.2f})"
            reason += f" with {separation_pct:.2f}% separation"
        elif signal == 'SELL':
            reason = f"Death cross: SMA{self.short_ma} (${sma_short:.2f}) < SMA{self.long_ma} (${sma_long:.2f})"
            reason += f" with {separation_pct:.2f}% separation"
        else:
            reason = f"No clear momentum signal - SMA{self.short_ma} ≈ SMA{self.long_ma}"

        # Add price position context
        if current_price > max(sma_short, sma_long):
            reason += f", price above both MAs"
        elif current_price < min(sma_short, sma_long):
            reason += f", price below both MAs"

        # Add sentiment context
        if abs(sentiment) > 0.3:
            sentiment_desc = "very positive" if sentiment > 0.5 else "positive" if sentiment > 0 else "negative" if sentiment > -0.5 else "very negative"
            reason += f", {sentiment_desc} sentiment"

        return reason

    def get_required_indicators(self) -> list[str]:
        """Return required technical indicators"""
        return [f'SMA_{self.short_ma}', f'SMA_{self.long_ma}']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""
        return StrategyMetadata(
            name="Moving Average Momentum",
            category="MOMENTUM",
            typical_holding_days=8,
            works_best_in="Trending markets with clear directional movement",
            min_confidence=65.0,
            max_positions=1,
            requires_pairs=False
        )
