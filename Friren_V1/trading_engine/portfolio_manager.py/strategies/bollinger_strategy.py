import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools

class BollingerStrategy(BaseStrategy):
    """
    Bollinger Bands Strategy

    Extracted from bolinger_bt.py - bb_backtest.backtest_bollinger_strategy()

    Logic:
    - BUY when price touches or goes below lower band (oversold)
    - SELL when price touches or goes above upper band (overbought)
    - Uses sentiment to distinguish between breakout vs mean reversion
    """

    def __init__(self, bb_period: int = 20, bb_std: float = 2.0):
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.data_tools = StockDataTools()
        super().__init__()

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """Generate Bollinger Bands signal"""

        # Validate required data
        if not self.validate_data(df, ['Close', 'High', 'Low']):
            return StrategySignal('HOLD', 0, 'Insufficient data for Bollinger Bands analysis')

        # Add Bollinger Bands if not present
        df_with_bb = self._ensure_bollinger_bands(df)

        if len(df_with_bb) < self.bb_period + 5:
            return StrategySignal('HOLD', 0, 'Insufficient data for Bollinger Bands calculation')

        # Get current values
        current = df_with_bb.iloc[-1]

        required_bb_cols = ['UpperBand', 'LowerBand', 'MA', 'BB_Position']
        if any(pd.isna(current[col]) for col in required_bb_cols):
            return StrategySignal('HOLD', 0, 'Bollinger Bands not yet calculated')

        current_price = current['Close']
        upper_band = current['UpperBand']
        lower_band = current['LowerBand']
        middle_band = current['MA']
        bb_position = current['BB_Position']
        bb_width = current.get('BB_Width', 0)

        # Calculate base signal
        base_signal, base_confidence = self._calculate_bollinger_signal(
            current_price, upper_band, lower_band, middle_band, bb_position, bb_width, df_with_bb
        )

        # Apply sentiment filter (Bollinger-specific)
        final_signal, final_confidence = self.apply_sentiment_filter(
            base_signal, base_confidence, sentiment
        )

        # Check confidence threshold
        if final_confidence < confidence_threshold:
            return StrategySignal(
                'HOLD',
                final_confidence,
                f'Bollinger signal below threshold: {final_confidence:.1f}% < {confidence_threshold}%'
            )

        # Generate reasoning
        reasoning = self._generate_reasoning(
            current_price, upper_band, lower_band, middle_band, bb_position,
            final_signal, final_confidence, sentiment
        )

        return StrategySignal(
            final_signal,
            final_confidence,
            reasoning,
            metadata={
                'price': current_price,
                'upper_band': upper_band,
                'lower_band': lower_band,
                'middle_band': middle_band,
                'bb_position': bb_position,
                'bb_width': bb_width,
                'sentiment_adjustment': final_confidence - base_confidence
            }
        )

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence based on band position and width"""

        if not self.validate_data(df, ['Close', 'High', 'Low']):
            return 0.0

        df_with_bb = self._ensure_bollinger_bands(df)

        if len(df_with_bb) < self.bb_period + 5:
            return 0.0

        current = df_with_bb.iloc[-1]

        if pd.isna(current.get('BB_Position')) or pd.isna(current.get('BB_Width')):
            return 0.0

        bb_position = current['BB_Position']
        bb_width = current.get('BB_Width', 0)

        # Confidence based on how close to band edges
        distance_from_center = abs(bb_position - 0.5)
        edge_confidence = min(100, distance_from_center * 200)  # 0.5 distance = 100% confidence

        # Add volatility context - wider bands = higher confidence in signals
        if bb_width > 0:
            recent_widths = df_with_bb['BB_Width'].tail(20)
            width_percentile = (recent_widths <= bb_width).mean()
            width_bonus = width_percentile * 20  # Up to 20% bonus for wide bands
        else:
            width_bonus = 0

        # Reduce confidence if bands are too narrow (low volatility)
        if bb_width < 0.03:  # Very narrow bands
            edge_confidence *= 0.7

        return min(100, edge_confidence + width_bonus)

    def _ensure_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has Bollinger Bands indicators"""
        df_copy = df.copy()

        bb_cols = ['UpperBand', 'LowerBand', 'MA', 'BB_Position', 'BB_Width']
        missing_bb = [col for col in bb_cols if col not in df_copy.columns]

        if missing_bb:
            df_copy = self.data_tools.add_bollinger_bands(
                df_copy,
                period=self.bb_period,
                num_std=self.bb_std
            )

        return df_copy

    def _calculate_bollinger_signal(self, current_price: float, upper_band: float,
                                  lower_band: float, middle_band: float, bb_position: float,
                                  bb_width: float, df_with_bb: pd.DataFrame) -> tuple[str, float]:
        """Calculate base Bollinger Bands signal"""

        base_signal = 'HOLD'
        base_confidence = 0.0

        # Lower band touch - potential buy (mean reversion)
        if current_price <= lower_band or bb_position <= 0.1:
            base_signal = 'BUY'
            # Confidence based on how far below lower band
            penetration = max(0, (lower_band - current_price) / lower_band)
            base_confidence = min(100, 60 + penetration * 200)

        # Upper band touch - potential sell (mean reversion)
        elif current_price >= upper_band or bb_position >= 0.9:
            base_signal = 'SELL'
            # Confidence based on how far above upper band
            penetration = max(0, (current_price - upper_band) / upper_band)
            base_confidence = min(100, 60 + penetration * 200)

        # Add volume confirmation if available
        if 'Volume' in df_with_bb.columns and base_signal != 'HOLD':
            volume_confirmation = self._check_volume_confirmation(df_with_bb, base_signal)
            base_confidence = min(100, base_confidence + volume_confirmation)

        # Add band width context
        if bb_width > 0:
            # Prefer signals when bands are wider (more volatile)
            recent_widths = df_with_bb['BB_Width'].tail(20)
            if len(recent_widths) > 5:
                width_percentile = (recent_widths <= bb_width).mean()
                if width_percentile > 0.7:  # Current width in top 30%
                    base_confidence = min(100, base_confidence * 1.15)
                elif width_percentile < 0.3:  # Current width in bottom 30%
                    base_confidence *= 0.85

        return base_signal, base_confidence

    def _check_volume_confirmation(self, df_with_bb: pd.DataFrame, signal: str) -> float:
        """Check for volume confirmation of signal"""
        if len(df_with_bb) < 10:
            return 0.0

        recent_data = df_with_bb.tail(5)
        avg_volume = df_with_bb['Volume'].tail(20).mean()
        current_volume = recent_data['Volume'].iloc[-1]

        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        # Higher volume confirms band touch signals
        if volume_ratio > 1.3:
            return 10.0  # 10% confidence bonus for high volume
        elif volume_ratio > 1.1:
            return 5.0   # 5% confidence bonus for above-average volume

        return 0.0

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> tuple[str, float]:
        """Apply Bollinger-specific sentiment filtering"""

        # Use sentiment to distinguish breakout vs mean reversion
        adjusted_confidence = base_confidence

        # Strong sentiment can indicate breakout rather than mean reversion
        if abs(sentiment) > 0.4:
            if base_signal == 'BUY' and sentiment > 0.4:
                # Strong positive sentiment + lower band touch might be breakout preparation
                adjusted_confidence *= 0.8  # Reduce mean reversion confidence
            elif base_signal == 'SELL' and sentiment < -0.4:
                # Strong negative sentiment + upper band touch might be breakout preparation
                adjusted_confidence *= 0.8  # Reduce mean reversion confidence

        # Moderate sentiment supports mean reversion
        elif 0.1 <= abs(sentiment) <= 0.3:
            if base_signal == 'BUY' and sentiment < 0:
                # Mild negative sentiment supports oversold mean reversion
                adjusted_confidence = min(100, adjusted_confidence * 1.1)
            elif base_signal == 'SELL' and sentiment > 0:
                # Mild positive sentiment supports overbought mean reversion
                adjusted_confidence = min(100, adjusted_confidence * 1.1)

        return base_signal, adjusted_confidence

    def _generate_reasoning(self, current_price: float, upper_band: float, lower_band: float,
                          middle_band: float, bb_position: float, signal: str,
                          confidence: float, sentiment: float) -> str:
        """Generate human-readable reasoning"""

        if signal == 'BUY':
            band_distance = (lower_band - current_price) / lower_band * 100
            if band_distance > 0:
                reason = f"Price ${current_price:.2f} below lower band ${lower_band:.2f} ({band_distance:.1f}%)"
            else:
                reason = f"Price ${current_price:.2f} at lower band ${lower_band:.2f}"
            reason += f" - oversold mean reversion opportunity"

        elif signal == 'SELL':
            band_distance = (current_price - upper_band) / upper_band * 100
            if band_distance > 0:
                reason = f"Price ${current_price:.2f} above upper band ${upper_band:.2f} ({band_distance:.1f}%)"
            else:
                reason = f"Price ${current_price:.2f} at upper band ${upper_band:.2f}"
            reason += f" - overbought mean reversion opportunity"

        else:
            reason = f"Price ${current_price:.2f} in middle of bands (${lower_band:.2f} - ${upper_band:.2f})"

        # Add band position context
        if bb_position is not None:
            position_pct = bb_position * 100
            reason += f", band position: {position_pct:.0f}%"

        # Add sentiment context
        if abs(sentiment) > 0.4:
            sentiment_desc = "strong positive" if sentiment > 0.4 else "strong negative"
            reason += f", {sentiment_desc} sentiment may indicate breakout vs reversion"
        elif abs(sentiment) > 0.1:
            sentiment_desc = "positive" if sentiment > 0 else "negative"
            reason += f", {sentiment_desc} sentiment supports mean reversion"

        return reason

    def get_required_indicators(self) -> list[str]:
        """Return required technical indicators"""
        return ['UpperBand', 'LowerBand', 'MA', 'BB_Position', 'BB_Width']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""
        return StrategyMetadata(
            name="Bollinger Bands Mean Reversion",
            category="MEAN_REVERSION",
            typical_holding_days=6,
            works_best_in="Markets with consistent volatility patterns and clear support/resistance",
            min_confidence=65.0,
            max_positions=1,
            requires_pairs=False
        )
