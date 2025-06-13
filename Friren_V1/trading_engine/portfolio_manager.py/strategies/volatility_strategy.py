import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools

class KalmanFilter:
    """
    Simplified Kalman Filter for price estimation
    Extracted from kalman_bt.py
    """
    def __init__(self, observation_covariance=1.0, transition_covariance=1.0):
        self.observation_covariance = observation_covariance
        self.transition_covariance = transition_covariance
        self.state_mean = 0.0
        self.state_covariance = 1.0

    def update(self, observation):
        """Update filter with new price observation"""
        # Prediction step
        predicted_state_mean = self.state_mean
        predicted_state_covariance = self.state_covariance + self.transition_covariance

        # Update step
        kalman_gain = predicted_state_covariance / (predicted_state_covariance + self.observation_covariance)

        self.state_mean = predicted_state_mean + kalman_gain * (observation - predicted_state_mean)
        self.state_covariance = (1 - kalman_gain) * predicted_state_covariance

        return {
            'fair_value': self.state_mean,
            'deviation': observation - self.state_mean,
            'confidence': 1.0 / self.state_covariance,
            'kalman_gain': kalman_gain
        }

class VolatilityStrategy(BaseStrategy):
    """
    Kalman Filter Volatility Strategy

    Extracted from kalman_bt.py - KalmanMeanReversionBacktest.backtest_kalman_strategy()

    Logic:
    - Uses Kalman filter to estimate "fair value" of price
    - BUY when price significantly below fair value (oversold by Z-score)
    - SELL when price reverts back towards fair value
    - Works best in high volatility mean-reverting markets
    """

    def __init__(self, entry_zscore: float = 1.5, exit_zscore: float = 0.3,
                 confidence_threshold_kalman: float = 0.05, lookback_window: int = 20):
        self.entry_zscore = entry_zscore
        self.exit_zscore = exit_zscore
        self.confidence_threshold_kalman = confidence_threshold_kalman
        self.lookback_window = lookback_window
        self.data_tools = StockDataTools()
        super().__init__()

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """Generate volatility-based mean reversion signal using Kalman filter"""

        # Validate required data
        if not self.validate_data(df, ['Close']):
            return StrategySignal('HOLD', 0, 'Insufficient data for volatility analysis')

        if len(df) < 30:
            return StrategySignal('HOLD', 0, 'Insufficient data for Kalman filter')

        # Calculate Kalman filter features
        df_with_kalman = self._add_kalman_features(df)

        if len(df_with_kalman) < self.lookback_window:
            return StrategySignal('HOLD', 0, 'Insufficient data for Z-score calculation')

        # Get current values
        current = df_with_kalman.iloc[-1]

        if pd.isna(current.get('kalman_zscore')) or pd.isna(current.get('kalman_confidence')):
            return StrategySignal('HOLD', 0, 'Kalman filter values not yet calculated')

        fair_value = current['kalman_fair_value']
        deviation = current['kalman_deviation']
        zscore = current['kalman_zscore']
        kalman_confidence = current['kalman_confidence']
        current_price = current['Close']

        # Check Kalman filter confidence threshold
        if kalman_confidence < self.confidence_threshold_kalman:
            return StrategySignal(
                'HOLD',
                20,  # Low confidence due to insufficient Kalman confidence
                f'Kalman filter confidence too low: {kalman_confidence:.3f} < {self.confidence_threshold_kalman}'
            )

        # Calculate base signal
        base_signal, base_confidence = self._calculate_kalman_signal(
            zscore, fair_value, current_price, deviation, kalman_confidence
        )

        # Apply sentiment filter (position sizing modifier, not signal filter)
        final_signal, final_confidence = self.apply_sentiment_filter(
            base_signal, base_confidence, sentiment
        )

        # Check confidence threshold
        if final_confidence < confidence_threshold:
            return StrategySignal(
                'HOLD',
                final_confidence,
                f'Kalman signal below threshold: {final_confidence:.1f}% < {confidence_threshold}%'
            )

        # Generate reasoning
        reasoning = self._generate_reasoning(
            current_price, fair_value, deviation, zscore, final_signal, final_confidence, sentiment
        )

        return StrategySignal(
            final_signal,
            final_confidence,
            reasoning,
            metadata={
                'price': current_price,
                'fair_value': fair_value,
                'deviation': deviation,
                'zscore': zscore,
                'kalman_confidence': kalman_confidence,
                'sentiment_adjustment': final_confidence - base_confidence
            }
        )

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence based on Kalman filter deviation and confidence"""

        if not self.validate_data(df, ['Close']):
            return 0.0

        if len(df) < 30:
            return 0.0

        df_with_kalman = self._add_kalman_features(df)

        if len(df_with_kalman) < self.lookback_window:
            return 0.0

        current = df_with_kalman.iloc[-1]

        if pd.isna(current.get('kalman_zscore')) or pd.isna(current.get('kalman_confidence')):
            return 0.0

        zscore = abs(current['kalman_zscore'])
        kalman_confidence = current['kalman_confidence']

        # Base confidence from Z-score magnitude
        zscore_confidence = min(100, zscore * 40)  # Z-score of 2.5 = 100% confidence

        # Scale by Kalman filter's own confidence
        kalman_factor = min(1.0, kalman_confidence / self.confidence_threshold_kalman)

        # Add volatility regime bonus
        if 'Volatility_20' not in df_with_kalman.columns:
            df_with_kalman = self.data_tools.add_volatility_features(df_with_kalman, windows=[20])

        recent_vol = df_with_kalman['Volatility_20'].iloc[-1]
        if not pd.isna(recent_vol):
            # Higher volatility = better for mean reversion strategy
            if recent_vol > 0.25:  # High volatility
                vol_bonus = 1.2
            elif recent_vol > 0.15:  # Medium volatility
                vol_bonus = 1.1
            else:  # Low volatility
                vol_bonus = 0.9
        else:
            vol_bonus = 1.0

        final_confidence = zscore_confidence * kalman_factor * vol_bonus
        return min(100, final_confidence)

    def _add_kalman_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Kalman filter features to dataframe"""
        df_kalman = df.copy()

        # Initialize Kalman filter
        kf = KalmanFilter(observation_cov=1.0, transition_cov=1.0)

        # Initialize columns
        df_kalman['kalman_fair_value'] = np.nan
        df_kalman['kalman_deviation'] = np.nan
        df_kalman['kalman_confidence'] = np.nan
        df_kalman['kalman_zscore'] = np.nan

        deviations = []

        for idx, row in df_kalman.iterrows():
            price = row['Close']
            result = kf.update(price)

            df_kalman.loc[idx, 'kalman_fair_value'] = result['fair_value']
            df_kalman.loc[idx, 'kalman_deviation'] = result['deviation']
            df_kalman.loc[idx, 'kalman_confidence'] = result['confidence']

            deviations.append(result['deviation'])

            # Calculate rolling Z-score of deviations
            if len(deviations) >= self.lookback_window:
                recent_deviations = deviations[-self.lookback_window:]
                std_dev = np.std(recent_deviations)
                if std_dev > 0:
                    zscore = result['deviation'] / std_dev
                    df_kalman.loc[idx, 'kalman_zscore'] = zscore

        return df_kalman

    def _calculate_kalman_signal(self, zscore: float, fair_value: float, current_price: float,
                               deviation: float, kalman_confidence: float) -> tuple[str, float]:
        """Calculate base Kalman filter signal"""

        base_signal = 'HOLD'
        base_confidence = 0.0

        # Entry signal: price significantly below fair value (oversold)
        if zscore <= -self.entry_zscore:
            base_signal = 'BUY'
            # Confidence increases with Z-score magnitude
            zscore_magnitude = abs(zscore)
            base_confidence = min(100, 50 + (zscore_magnitude - self.entry_zscore) * 30)

        # Exit signal: price reverted towards fair value
        elif zscore >= -self.exit_zscore and zscore <= self.exit_zscore:
            # This would be a sell signal if we had a position
            # For signal generation, we'll treat this as a weak sell signal
            if zscore > 0:  # Price above fair value
                base_signal = 'SELL'
                base_confidence = min(100, 40 + abs(zscore) * 20)

        # Scale confidence by Kalman filter's own confidence
        confidence_factor = min(1.5, kalman_confidence / self.confidence_threshold_kalman)
        base_confidence *= confidence_factor

        return base_signal, min(100, base_confidence)

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> tuple[str, float]:
        """Apply volatility strategy specific sentiment filtering"""

        # For volatility strategy, sentiment acts as position sizing modifier rather than signal filter
        # Strong negative sentiment increases risk, so reduce confidence
        adjusted_confidence = base_confidence

        if abs(sentiment) > 0.5:  # Strong sentiment = higher market risk
            adjusted_confidence *= 0.9  # Reduce confidence by 10%
        elif abs(sentiment) > 0.3:  # Moderate sentiment
            adjusted_confidence *= 0.95  # Reduce confidence by 5%

        # Don't change the signal, just adjust position sizing via confidence
        return base_signal, adjusted_confidence

    def _generate_reasoning(self, current_price: float, fair_value: float, deviation: float,
                          zscore: float, signal: str, confidence: float, sentiment: float) -> str:
        """Generate human-readable reasoning"""

        deviation_pct = (deviation / fair_value) * 100 if fair_value != 0 else 0

        if signal == 'BUY':
            reason = f"Kalman filter: price ${current_price:.2f} significantly below "
            reason += f"fair value ${fair_value:.2f} (Z-score: {zscore:.2f}, "
            reason += f"deviation: {deviation_pct:.1f}%) - oversold mean reversion opportunity"

        elif signal == 'SELL':
            reason = f"Kalman filter: price ${current_price:.2f} at/above "
            reason += f"fair value ${fair_value:.2f} (Z-score: {zscore:.2f}) - mean reversion complete"

        else:
            reason = f"Kalman filter: price ${current_price:.2f} near fair value ${fair_value:.2f} "
            reason += f"(Z-score: {zscore:.2f}) - no clear mean reversion signal"

        # Add sentiment context
        if abs(sentiment) > 0.3:
            sentiment_desc = "high positive" if sentiment > 0.5 else "positive" if sentiment > 0 else "negative" if sentiment > -0.5 else "high negative"
            reason += f", {sentiment_desc} sentiment increases market risk"

        return reason

    def get_required_indicators(self) -> list[str]:
        """Return required technical indicators (computed internally)"""
        return ['kalman_fair_value', 'kalman_deviation', 'kalman_zscore', 'kalman_confidence']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""
        return StrategyMetadata(
            name="Kalman Filter Volatility",
            category="VOLATILITY",
            typical_holding_days=4,
            works_best_in="High volatility markets with mean reversion tendencies",
            min_confidence=75.0,
            max_positions=1,
            requires_pairs=False
        )
