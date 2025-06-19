import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, List, Optional, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

from .base_strategy import BaseStrategy, StrategySignal, StrategyMetadata
from trading_engine.data.data_utils import StockDataTools

class EnhancedJumpDetector:
    """
    IMPROVED: Enhanced jump detection with multiple methods
    """

    def __init__(self, confidence_level: float = 0.99):
        self.confidence_level = confidence_level
        self.alpha = 1 - confidence_level

    def detect_jumps_multiple_methods(self, returns: pd.Series,
                                    window: int = 20) -> pd.DataFrame:
        """
        Apply multiple jump detection methods and combine results
        """
        jump_df = pd.DataFrame(index=returns.index)

        # Method 1: Simple threshold (your original method)
        jump_df['threshold_jumps'] = self._detect_threshold_jumps(returns, window)

        # Method 2: Lee-Mykland jump test (more sophisticated)
        jump_df['lee_mykland_jumps'] = self._detect_lee_mykland_jumps(returns, window)

        # Method 3: Barndorff-Nielsen-Shephard test
        jump_df['bns_jumps'] = self._detect_bns_jumps(returns, window)

        # Method 4: Outlier detection using IQR
        jump_df['iqr_jumps'] = self._detect_iqr_jumps(returns, window)

        # Combine methods (consensus approach)
        jump_df['consensus_jumps'] = (
            jump_df['threshold_jumps'] +
            jump_df['lee_mykland_jumps'] +
            jump_df['bns_jumps'] +
            jump_df['iqr_jumps']
        ) >= 2  # Require at least 2 methods to agree

        # Jump magnitude and direction
        jump_df['jump_magnitude'] = returns.abs()
        jump_df['jump_direction'] = np.sign(returns)

        # Jump significance score (0-1)
        jump_df['jump_significance'] = self._calculate_jump_significance(returns, window)

        return jump_df

    def _detect_threshold_jumps(self, returns: pd.Series, window: int) -> pd.Series:
        """Original threshold-based jump detection"""
        rolling_std = returns.rolling(window).std()
        z_scores = returns.abs() / rolling_std
        return (z_scores > 2.5).fillna(False)

    def _detect_lee_mykland_jumps(self, returns: pd.Series, window: int) -> pd.Series:
        """Lee-Mykland jump test"""
        # Estimate volatility using realized volatility
        rolling_var = returns.rolling(window).var()

        # Calculate test statistic
        test_stat = returns.abs() / np.sqrt(rolling_var)

        # Critical value (approximation)
        n = window
        beta_star = np.sqrt(2 * np.log(n))
        critical_value = beta_star - (np.log(np.pi) + np.log(np.log(n))) / (2 * beta_star)

        return (test_stat > critical_value).fillna(False)

    def _detect_bns_jumps(self, returns: pd.Series, window: int) -> pd.Series:
        """Simplified Barndorff-Nielsen-Shephard test"""
        # Calculate bipower variation (simplified)
        abs_returns = returns.abs()
        rolling_mean_abs = abs_returns.rolling(window).mean()
        rolling_std_abs = abs_returns.rolling(window).std()

        # Z-score based on absolute returns
        z_scores = (abs_returns - rolling_mean_abs) / rolling_std_abs

        return (z_scores > 2.0).fillna(False)

    def _detect_iqr_jumps(self, returns: pd.Series, window: int) -> pd.Series:
        """IQR-based outlier detection"""
        def iqr_outliers(x):
            if len(x) < 5:
                return False
            q75, q25 = np.percentile(x, [75, 25])
            iqr = q75 - q25
            lower_bound = q25 - 2.5 * iqr
            upper_bound = q75 + 2.5 * iqr
            return (x.iloc[-1] < lower_bound) or (x.iloc[-1] > upper_bound)

        return returns.rolling(window).apply(iqr_outliers, raw=False).fillna(False)

    def _calculate_jump_significance(self, returns: pd.Series, window: int) -> pd.Series:
        """Calculate jump significance score (0-1)"""
        rolling_std = returns.rolling(window).std()
        z_scores = returns.abs() / rolling_std

        # Convert Z-scores to significance using sigmoid
        significance = 1 / (1 + np.exp(-0.5 * (z_scores - 2)))
        return significance.fillna(0)

class JumpDiffusionStrategy(BaseStrategy):
    """
    IMPROVED: Jump Diffusion Strategy converted to production format

    Detects price jumps and trades based on:
    1. Jump detection using multiple methods
    2. Jump direction and magnitude
    3. Momentum confirmation
    4. Volatility regime awareness
    """

    def __init__(self, strategy_type: str = 'jump_momentum',
                 min_jump_threshold: float = 0.025,
                 momentum_window: int = 5,
                 detection_window: int = 20):
        """
        Args:
            strategy_type: 'jump_momentum', 'jump_reversal', 'volatility_breakout'
            min_jump_threshold: Minimum jump size to consider (e.g., 0.025 = 2.5%)
            momentum_window: Window for momentum calculation
            detection_window: Window for jump detection
        """
        self.strategy_type = strategy_type
        self.min_jump_threshold = min_jump_threshold
        self.momentum_window = momentum_window
        self.detection_window = detection_window

        self.data_tools = StockDataTools()
        self.jump_detector = EnhancedJumpDetector()

        super().__init__()

    def generate_signal(self, df: pd.DataFrame, sentiment: float = 0.0,
                       confidence_threshold: float = 60.0) -> StrategySignal:
        """Generate jump diffusion signal"""

        # Validate required data
        if not self.validate_data(df, ['Close', 'Volume']):
            return StrategySignal('HOLD', 0, 'Insufficient data for jump analysis')

        if len(df) < self.detection_window + 10:
            return StrategySignal('HOLD', 0, 'Insufficient data for jump detection')

        try:
            # Add enhanced features
            df_enhanced = self._add_jump_features(df)

            # Get current conditions
            current = df_enhanced.iloc[-1]
            recent_data = df_enhanced.tail(5)

            # Check for recent jumps
            recent_jumps = recent_data['consensus_jumps'].sum()
            if recent_jumps == 0:
                return StrategySignal('HOLD', 20, 'No recent jumps detected')

            # Calculate base signal
            base_signal, base_confidence = self._calculate_jump_signal(
                current, recent_data, df_enhanced
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
                    f'Jump signal below threshold: {final_confidence:.1f}% < {confidence_threshold}%'
                )

            # Generate reasoning
            reasoning = self._generate_jump_reasoning(
                current, final_signal, self.strategy_type, sentiment
            )

            return StrategySignal(
                final_signal,
                final_confidence,
                reasoning,
                metadata={
                    'strategy_type': self.strategy_type,
                    'jump_magnitude': current.get('jump_magnitude', 0),
                    'jump_significance': current.get('jump_significance', 0),
                    'momentum_signal': current.get('momentum_signal', 0),
                    'volatility_regime': current.get('volatility_regime', 'NORMAL')
                }
            )

        except Exception as e:
            return StrategySignal('HOLD', 0, f'Jump analysis error: {str(e)}')

    def get_signal_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence based on jump quality and market conditions"""

        try:
            if len(df) < self.detection_window + 10:
                return 0.0

            df_enhanced = self._add_jump_features(df)
            current = df_enhanced.iloc[-1]
            recent_data = df_enhanced.tail(10)

            # Base confidence from jump significance
            jump_significance = current.get('jump_significance', 0)
            base_confidence = jump_significance * 70  # Max 70% from jump quality

            # Momentum confirmation bonus
            momentum_signal = current.get('momentum_signal', 0)
            momentum_bonus = abs(momentum_signal) * 15  # Max 15% from momentum

            # Volatility regime adjustment
            volatility_regime = current.get('volatility_regime', 'NORMAL')
            if volatility_regime == 'HIGH':
                vol_adjustment = 10  # Bonus for high vol (jumps more meaningful)
            elif volatility_regime == 'LOW':
                vol_adjustment = -5   # Penalty for low vol (jumps less reliable)
            else:
                vol_adjustment = 0

            # Jump frequency penalty (too many jumps = noisy market)
            recent_jump_count = recent_data['consensus_jumps'].sum()
            if recent_jump_count > 5:  # Too many jumps
                frequency_penalty = 10
            else:
                frequency_penalty = 0

            final_confidence = base_confidence + momentum_bonus + vol_adjustment - frequency_penalty

            return max(0, min(100, final_confidence))

        except Exception:
            return 0.0

    def _add_jump_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add jump detection and momentum features"""
        df_enhanced = df.copy()

        # Calculate returns
        df_enhanced['returns'] = df_enhanced['Close'].pct_change()

        # Detect jumps using multiple methods
        jump_data = self.jump_detector.detect_jumps_multiple_methods(
            df_enhanced['returns'], self.detection_window
        )

        # Merge jump data
        for col in jump_data.columns:
            df_enhanced[col] = jump_data[col]

        # Add momentum features
        df_enhanced = self._add_momentum_features(df_enhanced)

        # Add volatility regime
        df_enhanced = self._add_volatility_regime(df_enhanced)

        # Add volume confirmation
        df_enhanced = self._add_volume_features(df_enhanced)

        return df_enhanced

    def _add_momentum_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add momentum and trend features"""
        df_momentum = df.copy()

        # Short and medium-term moving averages
        df_momentum['sma_short'] = df_momentum['Close'].rolling(self.momentum_window).mean()
        df_momentum['sma_medium'] = df_momentum['Close'].rolling(self.momentum_window * 2).mean()

        # Momentum signals
        df_momentum['momentum_signal'] = 0
        df_momentum['trend_strength'] = 0

        # Calculate momentum
        for i in range(self.momentum_window * 2, len(df_momentum)):
            current_price = df_momentum['Close'].iloc[i]
            sma_short = df_momentum['sma_short'].iloc[i]
            sma_medium = df_momentum['sma_medium'].iloc[i]

            if pd.notna(sma_short) and pd.notna(sma_medium):
                # Momentum direction
                if current_price > sma_short > sma_medium:
                    df_momentum.iloc[i, df_momentum.columns.get_loc('momentum_signal')] = 1
                    df_momentum.iloc[i, df_momentum.columns.get_loc('trend_strength')] = \
                        (current_price - sma_medium) / sma_medium
                elif current_price < sma_short < sma_medium:
                    df_momentum.iloc[i, df_momentum.columns.get_loc('momentum_signal')] = -1
                    df_momentum.iloc[i, df_momentum.columns.get_loc('trend_strength')] = \
                        (current_price - sma_medium) / sma_medium

        return df_momentum

    def _add_volatility_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add volatility regime classification"""
        df_vol = df.copy()

        # Calculate rolling volatility
        df_vol['rolling_vol'] = df_vol['returns'].rolling(self.detection_window).std() * np.sqrt(252)

        # Volatility regime
        df_vol['volatility_regime'] = 'NORMAL'

        for i in range(self.detection_window * 2, len(df_vol)):
            current_vol = df_vol['rolling_vol'].iloc[i]
            vol_history = df_vol['rolling_vol'].iloc[i-self.detection_window*2:i]

            if pd.notna(current_vol) and len(vol_history.dropna()) > 10:
                vol_percentile = (vol_history <= current_vol).mean()

                if vol_percentile > 0.8:
                    df_vol.iloc[i, df_vol.columns.get_loc('volatility_regime')] = 'HIGH'
                elif vol_percentile < 0.2:
                    df_vol.iloc[i, df_vol.columns.get_loc('volatility_regime')] = 'LOW'

        return df_vol

    def _add_volume_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add volume confirmation features"""
        df_volume = df.copy()

        # Volume moving average and ratio
        df_volume['volume_ma'] = df_volume['Volume'].rolling(self.detection_window).mean()
        df_volume['volume_ratio'] = df_volume['Volume'] / df_volume['volume_ma']

        # Volume confirmation signal
        df_volume['volume_confirmation'] = (df_volume['volume_ratio'] > 1.2).astype(int)

        return df_volume

    def _calculate_jump_signal(self, current: pd.Series, recent_data: pd.DataFrame,
                             df_enhanced: pd.DataFrame) -> Tuple[str, float]:
        """Calculate signal based on jump detection and strategy type"""

        base_signal = 'HOLD'
        base_confidence = 0.0

        # Get current conditions
        jump_detected = current.get('consensus_jumps', False)
        jump_magnitude = current.get('jump_magnitude', 0)
        jump_direction = current.get('jump_direction', 0)
        jump_significance = current.get('jump_significance', 0)
        momentum_signal = current.get('momentum_signal', 0)
        volume_confirmation = current.get('volume_confirmation', False)

        # Only proceed if jump is significant enough
        if not jump_detected or jump_magnitude < self.min_jump_threshold:
            return base_signal, base_confidence

        if self.strategy_type == 'jump_momentum':
            # Follow jump direction if momentum agrees
            if jump_direction > 0 and momentum_signal >= 0:
                base_signal = 'BUY'
                base_confidence = min(100, jump_significance * 80 + momentum_signal * 20)
            elif jump_direction < 0 and momentum_signal <= 0:
                base_signal = 'SELL'
                base_confidence = min(100, jump_significance * 80 + abs(momentum_signal) * 20)

        elif self.strategy_type == 'jump_reversal':
            # Bet against the jump (mean reversion)
            if jump_direction > 0:  # Price jumped up, expect reversal
                base_signal = 'SELL'
                base_confidence = min(100, jump_significance * 60)
            elif jump_direction < 0:  # Price jumped down, expect bounce
                base_signal = 'BUY'
                base_confidence = min(100, jump_significance * 60)

        elif self.strategy_type == 'volatility_breakout':
            # Buy on any significant jump with volume confirmation
            if volume_confirmation and jump_significance > 0.7:
                if jump_direction > 0:
                    base_signal = 'BUY'
                else:
                    base_signal = 'SELL'
                base_confidence = min(100, jump_significance * 70 + 20)

        # Volume confirmation bonus
        if volume_confirmation and base_signal != 'HOLD':
            base_confidence = min(100, base_confidence * 1.15)

        return base_signal, base_confidence

    def apply_sentiment_filter(self, base_signal: str, base_confidence: float,
                             sentiment: float) -> Tuple[str, float]:
        """Apply jump-specific sentiment filtering"""

        adjusted_confidence = base_confidence

        # Jump strategies can work against sentiment in short term
        # But extreme sentiment provides context

        if self.strategy_type == 'jump_momentum':
            # Momentum strategies benefit from sentiment alignment
            if base_signal == 'BUY' and sentiment > 0.2:
                adjusted_confidence = min(100, adjusted_confidence * 1.15)
            elif base_signal == 'SELL' and sentiment < -0.2:
                adjusted_confidence = min(100, adjusted_confidence * 1.15)
            elif base_signal == 'BUY' and sentiment < -0.5:
                adjusted_confidence *= 0.8  # Strong negative sentiment vs bullish jump
            elif base_signal == 'SELL' and sentiment > 0.5:
                adjusted_confidence *= 0.8  # Strong positive sentiment vs bearish jump

        elif self.strategy_type == 'jump_reversal':
            # Reversal strategies work better against extreme sentiment
            if abs(sentiment) > 0.5:
                adjusted_confidence = min(100, adjusted_confidence * 1.2)
            elif abs(sentiment) < 0.1:
                adjusted_confidence *= 0.9  # Need some sentiment extreme for reversal

        elif self.strategy_type == 'volatility_breakout':
            # Volatility strategies less affected by sentiment
            if abs(sentiment) > 0.7:  # Only adjust for extreme sentiment
                adjusted_confidence *= 0.95

        return base_signal, adjusted_confidence

    def _generate_jump_reasoning(self, current: pd.Series, signal: str,
                               strategy_type: str, sentiment: float) -> str:
        """Generate human-readable reasoning"""

        jump_magnitude = current.get('jump_magnitude', 0) * 100
        jump_direction = current.get('jump_direction', 0)
        jump_significance = current.get('jump_significance', 0)
        momentum_signal = current.get('momentum_signal', 0)
        volatility_regime = current.get('volatility_regime', 'NORMAL')

        direction_desc = "upward" if jump_direction > 0 else "downward"

        if strategy_type == 'jump_momentum':
            reason = f"Jump momentum: {direction_desc} jump of {jump_magnitude:.1f}% "
            reason += f"with {'positive' if momentum_signal > 0 else 'negative' if momentum_signal < 0 else 'neutral'} momentum"

        elif strategy_type == 'jump_reversal':
            reason = f"Jump reversal: {direction_desc} jump of {jump_magnitude:.1f}% "
            reason += f"expected to reverse (significance: {jump_significance:.2f})"

        elif strategy_type == 'volatility_breakout':
            reason = f"Volatility breakout: significant {direction_desc} jump of {jump_magnitude:.1f}% "
            reason += f"in {volatility_regime.lower()} volatility regime"

        else:
            reason = f"Jump detected: {direction_desc} move of {jump_magnitude:.1f}%"

        # Add sentiment context
        if abs(sentiment) > 0.3:
            sentiment_desc = "positive" if sentiment > 0 else "negative"
            reason += f", {sentiment_desc} sentiment provides context"

        return reason

    def get_required_indicators(self) -> List[str]:
        """Return required indicators (computed internally)"""
        return ['consensus_jumps', 'jump_magnitude', 'jump_significance', 'momentum_signal']

    def _get_strategy_metadata(self) -> StrategyMetadata:
        """Return strategy metadata"""

        category_map = {
            'jump_momentum': 'MOMENTUM',
            'jump_reversal': 'MEAN_REVERSION',
            'volatility_breakout': 'VOLATILITY'
        }

        works_best_desc = {
            'jump_momentum': 'Trending markets with clear momentum after price shocks',
            'jump_reversal': 'Range-bound markets where jumps represent temporary dislocations',
            'volatility_breakout': 'High volatility markets with significant price movements'
        }

        return StrategyMetadata(
            name=f"Jump Diffusion {self.strategy_type.replace('_', ' ').title()}",
            category=category_map.get(self.strategy_type, 'VOLATILITY'),
            typical_holding_days=3,
            works_best_in=works_best_desc.get(self.strategy_type, 'Markets with significant price jumps'),
            min_confidence=70.0,
            max_positions=1,
            requires_pairs=False
        )
