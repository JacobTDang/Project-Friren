import sys
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from scipy import stats
from sklearn.feature_selection import mutual_info_regression
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

# Path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from trading_engine.data.yahoo import StockDataTools

@dataclass
class EntropyConfig:
    """Configuration for entropy-based regime detection parameters"""
    # Entropy calculation windows
    price_entropy_window: int = 20
    volume_entropy_window: int = 15
    volatility_entropy_window: int = 10

    # Return distribution analysis
    return_bins: int = 10
    distribution_window: int = 50

    # Regime classification thresholds
    high_entropy_threshold: float = 0.75  # 75th percentile
    low_entropy_threshold: float = 0.25   # 25th percentile

    # Market microstructure parameters
    intraday_bins: int = 8
    price_level_bins: int = 12

    # Regime detection sensitivity
    regime_persistence: int = 3  # Minimum days to confirm regime change
    confidence_threshold: float = 0.6

    # Cross-validation parameters
    lookback_periods: List[int] = None
    entropy_smoothing: float = 0.3  # EWM alpha for smoothing

    def __post_init__(self):
        if self.lookback_periods is None:
            self.lookback_periods = [10, 20, 30, 50]

class EntropyRegimeDetector:
    """
    Advanced regime detection using information theory and entropy measures

    This detector identifies market regimes based on:
    1. Price movement entropy (randomness vs trending)
    2. Volume distribution entropy (participation patterns)
    3. Volatility clustering entropy (risk regime identification)
    4. Return distribution entropy (tail risk assessment)
    """

    def __init__(self, config: Optional[EntropyConfig] = None):
        self.config = config or EntropyConfig()
        self.SDT = StockDataTools()
        self.regime_history = []
        self.entropy_history = []

    def add_entropy_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add comprehensive entropy-based indicators for regime detection
        """
        df = df.copy()

        # Basic market data preparation
        df = self._prepare_market_data(df)

        # Core entropy measures
        df = self._add_price_entropy(df)
        df = self._add_volume_entropy(df)
        df = self._add_volatility_entropy(df)
        df = self._add_return_distribution_entropy(df)

        # Advanced entropy measures
        df = self._add_microstructure_entropy(df)
        df = self._add_cross_entropy_measures(df)

        # Composite entropy scores
        df = self._add_composite_entropy_scores(df)

        return df

    def _prepare_market_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare basic market data and derived features
        """
        df = df.copy()

        # Basic returns and log returns
        df['Returns'] = df['Close'].pct_change()
        df['Log_Returns'] = np.log(df['Close'] / df['Close'].shift(1))

        # Intraday measures
        df['Range'] = (df['High'] - df['Low']) / df['Close']
        df['Body'] = abs(df['Close'] - df['Open']) / df['Close']
        df['Upper_Shadow'] = (df['High'] - np.maximum(df['Open'], df['Close'])) / df['Close']
        df['Lower_Shadow'] = (np.minimum(df['Open'], df['Close']) - df['Low']) / df['Close']

        # Volume measures
        df['Volume_Ratio'] = df['Volume'] / df['Volume'].rolling(20).mean()
        df['Price_Volume'] = df['Close'] * df['Volume']

        return df

    def _add_price_entropy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate entropy of price movements to detect trending vs random behavior
        """
        df = df.copy()
        window = self.config.price_entropy_window

        # Price direction entropy
        df['Price_Direction'] = np.sign(df['Returns'])
        df['Direction_Entropy_raw'] = df['Price_Direction'].rolling(window).apply(
            lambda x: self._calculate_categorical_entropy(x), raw=True
        )

        # Price magnitude entropy (binned returns)
        df['Return_Magnitude_Entropy_raw'] = df['Returns'].rolling(window).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=self.config.return_bins), raw=True
        )

        # High-Low range entropy
        df['Range_Entropy_raw'] = df['Range'].rolling(window).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=8), raw=True
        )

        # Intraday pattern entropy
        df['Intraday_Pattern'] = (df['Close'] - df['Open']) / (df['Range'] + 1e-8)
        df['Intraday_Entropy_raw'] = df['Intraday_Pattern'].rolling(window).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=6), raw=True
        )

        return df

    def _add_volume_entropy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate entropy of volume patterns to detect participation regimes
        """
        df = df.copy()
        window = self.config.volume_entropy_window

        # Volume magnitude entropy
        df['Volume_Entropy_raw'] = df['Volume'].rolling(window).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=8), raw=True
        )

        # Volume-price relationship entropy
        df['VP_Correlation'] = df['Returns'].rolling(window).corr(df['Volume_Ratio'])
        df['VP_Entropy_raw'] = df['VP_Correlation'].rolling(window).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=6), raw=True
        )

        # Volume distribution across price levels
        df['Volume_Price_Entropy_raw'] = self._calculate_volume_price_entropy(df, window)

        return df

    def _add_volatility_entropy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate entropy of volatility clustering patterns
        """
        df = df.copy()
        window = self.config.volatility_entropy_window

        # Realized volatility entropy
        df['Realized_Vol'] = df['Returns'].rolling(window).std()
        df['Vol_Entropy_raw'] = df['Realized_Vol'].rolling(window * 2).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=6), raw=True
        )

        # GARCH-like volatility clustering
        df['Vol_Clustering'] = (df['Returns'] ** 2).rolling(window).mean()
        df['Clustering_Entropy_raw'] = df['Vol_Clustering'].rolling(window * 2).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=6), raw=True
        )

        # Volatility regime persistence
        try:
            # Create volatility regime categories using forward fill
            vol_filled = df['Realized_Vol'].fillna(method='ffill')
            if len(vol_filled.dropna()) > 10:  # Need enough data for quantiles
                df['Vol_Regime'] = pd.qcut(vol_filled, q=3, labels=['Low', 'Medium', 'High'], duplicates='drop')

                # Convert categorical to numeric for rolling operations
                regime_numeric = df['Vol_Regime'].cat.codes.astype(float)
                regime_numeric = regime_numeric.replace(-1, np.nan)  # Replace unassigned categories

                df['Vol_Regime_Entropy_raw'] = regime_numeric.rolling(window * 2).apply(
                    lambda x: self._calculate_categorical_entropy(x), raw=True
                )
            else:
                # Fallback for insufficient data
                df['Vol_Regime'] = 'Medium'
                df['Vol_Regime_Entropy_raw'] = 0.5
        except Exception as e:
            print(f"Warning: Volatility regime calculation failed: {e}")
            df['Vol_Regime'] = 'Medium'
            df['Vol_Regime_Entropy_raw'] = 0.5

        return df

    def _add_return_distribution_entropy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze entropy of return distributions for tail risk assessment
        """
        df = df.copy()
        window = self.config.distribution_window

        # Rolling distribution entropy
        df['Distribution_Entropy_raw'] = df['Returns'].rolling(window).apply(
            lambda x: self._calculate_distribution_entropy(x), raw=True
        )

        # Tail entropy (focus on extreme movements)
        df['Tail_Entropy_raw'] = df['Returns'].rolling(window).apply(
            lambda x: self._calculate_tail_entropy(x), raw=True
        )

        # Skewness and kurtosis entropy
        df['Skewness'] = df['Returns'].rolling(window).skew()
        df['Kurtosis'] = df['Returns'].rolling(window).kurt()

        # Create a combined moment entropy measure
        try:
            df['Moment_Entropy_raw'] = df.apply(
                lambda row: self._calculate_continuous_entropy([row['Skewness'], row['Kurtosis']], bins=6)
                if not pd.isna(row['Skewness']) and not pd.isna(row['Kurtosis']) else 0, axis=1
            )
        except:
            df['Moment_Entropy_raw'] = 0.5

        return df

    def _add_microstructure_entropy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate microstructure entropy measures
        """
        df = df.copy()

        # Order flow entropy (approximated)
        df['Order_Flow'] = np.sign(df['Close'] - df['Close'].shift(1)) * df['Volume']
        df['Order_Flow_Entropy_raw'] = df['Order_Flow'].rolling(20).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=8), raw=True
        )

        # Bid-ask spread proxy entropy
        df['Spread_Proxy'] = (df['High'] - df['Low']) / df['Close']
        df['Spread_Entropy_raw'] = df['Spread_Proxy'].rolling(15).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=6), raw=True
        )

        # Market impact entropy
        df['Market_Impact'] = abs(df['Returns']) / (df['Volume_Ratio'] + 1e-8)
        df['Impact_Entropy_raw'] = df['Market_Impact'].rolling(20).apply(
            lambda x: self._calculate_continuous_entropy(x, bins=8), raw=True
        )

        return df

    def _add_cross_entropy_measures(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate cross-entropy measures using mutual information between market dimensions
        """
        df = df.copy()

        # Price-Volume mutual information (more precise than cross-entropy)
        df['PV_Mutual_Info_raw'] = self._calculate_mutual_information(
            df['Returns'], df['Volume_Ratio'], window=30
        )

        # Volatility-Volume mutual information
        df['VV_Mutual_Info_raw'] = self._calculate_mutual_information(
            df['Realized_Vol'], df['Volume'], window=25
        )

        # Range-Volume mutual information
        df['RV_Mutual_Info_raw'] = self._calculate_mutual_information(
            df['Range'], df['Volume'], window=20
        )

        return df

    def _add_composite_entropy_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create composite entropy scores with slope detection and Z-score standardization
        """
        df = df.copy()

        # Normalize entropy measures to percentiles with consistent naming
        entropy_raw_columns = [col for col in df.columns if col.endswith('_raw')]

        for col in entropy_raw_columns:
            if col in df.columns:
                # Create percentile version
                pct_col = col.replace('_raw', '_pct')
                df[pct_col] = df[col].rolling(252, min_periods=50).rank(pct=True)

                # Create smoothed version
                ewm_col = col.replace('_raw', '_ewm')
                df[ewm_col] = df[col].ewm(alpha=self.config.entropy_smoothing).mean()

                # Create Z-score standardized version for ML use
                zscore_col = col.replace('_raw', '_zscore')
                rolling_mean = df[col].rolling(126, min_periods=30).mean()
                rolling_std = df[col].rolling(126, min_periods=30).std()
                df[zscore_col] = (df[col] - rolling_mean) / (rolling_std + 1e-8)

        # Composite entropy score using percentile versions
        primary_entropy_cols = [
            'Direction_Entropy_pct',
            'Return_Magnitude_Entropy_pct',
            'Volume_Entropy_pct',
            'Vol_Entropy_pct',
            'Distribution_Entropy_pct'
        ]

        available_cols = [col for col in primary_entropy_cols if col in df.columns]
        if available_cols:
            df['Composite_Entropy_raw'] = df[available_cols].mean(axis=1)
        else:
            df['Composite_Entropy_raw'] = 0.5

        # Smoothed composite entropy for regime detection
        df['Composite_Entropy_ewm'] = df['Composite_Entropy_raw'].ewm(
            alpha=self.config.entropy_smoothing
        ).mean()

        # Percentile version of composite entropy
        df['Composite_Entropy_pct'] = df['Composite_Entropy_raw'].rolling(
            252, min_periods=50
        ).rank(pct=True)

        # Z-score version for ML
        rolling_mean = df['Composite_Entropy_raw'].rolling(126, min_periods=30).mean()
        rolling_std = df['Composite_Entropy_raw'].rolling(126, min_periods=30).std()
        df['Composite_Entropy_zscore'] = (df['Composite_Entropy_raw'] - rolling_mean) / (rolling_std + 1e-8)

        # Entropy slope for regime change detection
        df['Composite_Entropy_slope'] = df['Composite_Entropy_ewm'].diff()
        df['Entropy_Acceleration'] = df['Composite_Entropy_slope'].diff()

        return df

    def detect_regime_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect regime history using entropy-based analysis with robust regime persistence
        """
        # Add entropy indicators
        df_enhanced = self.add_entropy_indicators(df)

        # Initialize regime columns
        df_enhanced['Entropy_Regime_raw'] = 'UNKNOWN'
        df_enhanced['Entropy_Confidence'] = 0.0
        df_enhanced['Regime_Scores'] = None

        # Start after sufficient data for entropy calculation
        start_idx = max(60, len(df_enhanced) // 6)

        regime_history = []

        for i in range(start_idx, len(df_enhanced)):
            # Detect regime for current point
            regime_info = self._detect_single_entropy_regime(df_enhanced, i)

            # Store raw regime detection
            df_enhanced.iloc[i, df_enhanced.columns.get_loc('Entropy_Regime_raw')] = regime_info['regime']
            df_enhanced.iloc[i, df_enhanced.columns.get_loc('Entropy_Confidence')] = regime_info['confidence']
            df_enhanced.iloc[i, df_enhanced.columns.get_loc('Regime_Scores')] = str(regime_info['scores'])

            # Store in history
            regime_history.append({
                'date': df_enhanced.index[i],
                'regime': regime_info['regime'],
                'confidence': regime_info['confidence'],
                **regime_info['entropy_measures']
            })

        # Apply regime persistence smoothing
        df_enhanced['Entropy_Regime'] = self._apply_regime_persistence_smoothing(
            df_enhanced['Entropy_Regime_raw']
        )

        self.regime_history = regime_history
        return df_enhanced

    def _apply_regime_persistence_smoothing(self, regime_series: pd.Series) -> pd.Series:
        """
        FIXED: Apply regime persistence smoothing with numeric mapping for rolling operations
        """
        persistence = self.config.regime_persistence

        # Get unique regimes (excluding UNKNOWN for mapping)
        unique_regimes = sorted([r for r in regime_series.dropna().unique() if r != 'UNKNOWN'])

        if not unique_regimes:
            # If no valid regimes found, return original series
            return regime_series.fillna('UNKNOWN')

        # Create mapping: regime -> numeric code
        regime_to_code = {regime: i for i, regime in enumerate(unique_regimes)}
        code_to_regime = {i: regime for regime, i in regime_to_code.items()}

        # Convert regimes to numeric codes (UNKNOWN becomes NaN)
        numeric_series = regime_series.map(regime_to_code)

        # Apply rolling mode with numeric data
        def rolling_mode(x):
            """Calculate mode of numeric codes, ignoring NaN"""
            x_clean = pd.Series(x).dropna()
            if len(x_clean) == 0:
                return np.nan
            return x_clean.mode().iloc[0] if len(x_clean.mode()) > 0 else np.nan

        smoothed_numeric = numeric_series.rolling(
            window=persistence,
            min_periods=1
        ).apply(rolling_mode, raw=False)

        # Convert back to regime strings
        smoothed_regimes = smoothed_numeric.map(code_to_regime)

        # Forward fill stable regimes and fill remaining NaN with 'UNKNOWN'
        smoothed_regimes = smoothed_regimes.fillna(method='ffill').fillna('UNKNOWN')

        return smoothed_regimes

    def _detect_single_entropy_regime(self, df: pd.DataFrame, idx: int) -> Dict:
        """
        Detect regime at a single point using entropy analysis
        """
        if idx < 60:
            return {
                'regime': 'UNKNOWN',
                'confidence': 0.0,
                'scores': {},
                'entropy_measures': {}
            }

        current = df.iloc[idx]

        # Initialize regime scores
        regime_scores = {
            'TRENDING': 0,
            'MEAN_REVERTING': 0,
            'HIGH_ENTROPY': 0,
            'LOW_ENTROPY': 0,
            'VOLATILE': 0,
            'STABLE': 0
        }

        # 1. DIRECTIONAL ENTROPY ANALYSIS
        direction_entropy = current.get('Direction_Entropy_pct', 0.5)
        if not pd.isna(direction_entropy):
            if direction_entropy < self.config.low_entropy_threshold:
                regime_scores['TRENDING'] += 3  # Low directional entropy = trending
            elif direction_entropy > self.config.high_entropy_threshold:
                regime_scores['MEAN_REVERTING'] += 3  # High directional entropy = random/mean reverting

        # 2. MAGNITUDE ENTROPY ANALYSIS
        magnitude_entropy = current.get('Return_Magnitude_Entropy_pct', 0.5)
        if not pd.isna(magnitude_entropy):
            if magnitude_entropy < self.config.low_entropy_threshold:
                regime_scores['STABLE'] += 2  # Consistent return magnitudes
            elif magnitude_entropy > self.config.high_entropy_threshold:
                regime_scores['VOLATILE'] += 2  # Varied return magnitudes

        # 3. VOLUME ENTROPY ANALYSIS
        volume_entropy = current.get('Volume_Entropy_pct', 0.5)
        if not pd.isna(volume_entropy):
            if volume_entropy < self.config.low_entropy_threshold:
                regime_scores['STABLE'] += 2  # Consistent volume patterns
            elif volume_entropy > self.config.high_entropy_threshold:
                regime_scores['HIGH_ENTROPY'] += 2  # Chaotic volume patterns

        # 4. VOLATILITY ENTROPY ANALYSIS
        vol_entropy = current.get('Vol_Entropy_pct', 0.5)
        if not pd.isna(vol_entropy):
            if vol_entropy < self.config.low_entropy_threshold:
                regime_scores['STABLE'] += 2  # Stable volatility regime
            elif vol_entropy > self.config.high_entropy_threshold:
                regime_scores['VOLATILE'] += 3  # Volatile regime transitions

        # 5. DISTRIBUTION ENTROPY ANALYSIS
        dist_entropy = current.get('Distribution_Entropy_pct', 0.5)
        if not pd.isna(dist_entropy):
            if dist_entropy < self.config.low_entropy_threshold:
                regime_scores['LOW_ENTROPY'] += 2  # Predictable distribution
            elif dist_entropy > self.config.high_entropy_threshold:
                regime_scores['HIGH_ENTROPY'] += 3  # Complex distribution patterns

        # 6. MUTUAL INFORMATION MEASURES
        pv_mutual_info = current.get('PV_Mutual_Info_raw', 0)
        if not pd.isna(pv_mutual_info):
            if pv_mutual_info > 0.7:
                regime_scores['HIGH_ENTROPY'] += 1
            elif pv_mutual_info < 0.3:
                regime_scores['LOW_ENTROPY'] += 1

        # 7. COMPOSITE ENTROPY ASSESSMENT
        composite_entropy = current.get('Composite_Entropy_ewm', 0.5)
        if not pd.isna(composite_entropy):
            if composite_entropy < 0.3:
                regime_scores['LOW_ENTROPY'] += 2
                regime_scores['TRENDING'] += 1
            elif composite_entropy > 0.7:
                regime_scores['HIGH_ENTROPY'] += 2
                regime_scores['MEAN_REVERTING'] += 1

        # Determine primary regime
        primary_regime = max(regime_scores, key=regime_scores.get)

        # Calculate confidence
        max_score = regime_scores[primary_regime]
        total_possible = 15.0  # Maximum possible score
        confidence = min(max_score / total_possible, 1.0)

        # Boost confidence based on score separation
        sorted_scores = sorted(regime_scores.values(), reverse=True)
        if len(sorted_scores) >= 2:
            margin = sorted_scores[0] - sorted_scores[1]
            confidence = min(confidence + (margin / total_possible), 1.0)

        return {
            'regime': primary_regime,
            'confidence': confidence,
            'scores': regime_scores,
            'entropy_measures': {
                'direction_entropy': direction_entropy,
                'magnitude_entropy': magnitude_entropy,
                'volume_entropy': volume_entropy,
                'vol_entropy': vol_entropy,
                'distribution_entropy': dist_entropy,
                'composite_entropy': composite_entropy
            }
        }

    def get_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate exportable trading signals based on entropy regime detection
        """
        # Run full regime detection
        df_with_regimes = self.detect_regime_history(df)

        # Create signals dataframe
        signals_df = pd.DataFrame(index=df.index)
        signals_df['Regime'] = df_with_regimes['Entropy_Regime']
        signals_df['Regime_Confidence'] = df_with_regimes['Entropy_Confidence']
        signals_df['Composite_Entropy'] = df_with_regimes.get('Composite_Entropy_ewm', 0)
        signals_df['Entropy_Slope'] = df_with_regimes.get('Composite_Entropy_slope', 0)

        # Detect regime transitions
        signals_df['Previous_Regime'] = signals_df['Regime'].shift(1)
        signals_df['Regime_Changed'] = (signals_df['Regime'] != signals_df['Previous_Regime'])

        # Generate trading signals based on regime
        signals_df['Signal'] = 'HOLD'

        # Signal logic based on entropy regimes
        trending_mask = signals_df['Regime'] == 'TRENDING'
        mean_reverting_mask = signals_df['Regime'] == 'MEAN_REVERTING'
        low_entropy_mask = signals_df['Regime'] == 'LOW_ENTROPY'
        high_entropy_mask = signals_df['Regime'] == 'HIGH_ENTROPY'
        stable_mask = signals_df['Regime'] == 'STABLE'
        volatile_mask = signals_df['Regime'] == 'VOLATILE'

        # High confidence regime entries
        high_conf_mask = signals_df['Regime_Confidence'] > self.config.confidence_threshold
        regime_entry_mask = signals_df['Regime_Changed'] & high_conf_mask

        # TRENDING regime: momentum signals
        signals_df.loc[trending_mask & regime_entry_mask & (signals_df['Entropy_Slope'] > 0), 'Signal'] = 'ENTER_MOMENTUM'

        # MEAN_REVERTING regime: mean reversion signals
        signals_df.loc[mean_reverting_mask & regime_entry_mask, 'Signal'] = 'ENTER_MEAN_REVERSION'

        # LOW_ENTROPY regime: trend following
        signals_df.loc[low_entropy_mask & regime_entry_mask, 'Signal'] = 'ENTER_TREND_FOLLOW'

        # HIGH_ENTROPY regime: reduce exposure
        signals_df.loc[high_entropy_mask & regime_entry_mask, 'Signal'] = 'REDUCE_EXPOSURE'

        # VOLATILE regime: exit positions
        signals_df.loc[volatile_mask & regime_entry_mask, 'Signal'] = 'EXIT_POSITIONS'

        # STABLE regime: increase exposure
        signals_df.loc[stable_mask & regime_entry_mask, 'Signal'] = 'INCREASE_EXPOSURE'

        # Regime exit signals (low confidence or regime ending)
        low_conf_mask = signals_df['Regime_Confidence'] < (self.config.confidence_threshold * 0.7)
        regime_deteriorating = (signals_df['Entropy_Slope'].abs() > 0.1) & low_conf_mask

        signals_df.loc[regime_deteriorating, 'Signal'] = 'PREPARE_EXIT'

        # Add signal strength and grading
        signals_df['Signal_Strength'] = signals_df['Regime_Confidence'] * (1 + abs(signals_df['Entropy_Slope']))

        # Signal grading for prioritization
        try:
            signals_df['Signal_Grade'] = pd.qcut(
                signals_df['Signal_Strength'].fillna(0),
                q=3,
                labels=['Weak', 'Medium', 'Strong'],
                duplicates='drop'
            ).fillna('Weak')
        except:
            signals_df['Signal_Grade'] = 'Medium'

        # Clean up intermediate columns
        signals_df = signals_df.drop(['Previous_Regime'], axis=1)

        return signals_df

    def get_regime_transition_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Focus specifically on regime transition detection for early warning
        """
        df_with_entropy = self.add_entropy_indicators(df)

        # Calculate transition indicators
        transition_df = pd.DataFrame(index=df.index)
        transition_df['Entropy_Acceleration'] = df_with_entropy.get('Entropy_Acceleration', 0)
        transition_df['Composite_Entropy'] = df_with_entropy.get('Composite_Entropy_ewm', 0.5)
        transition_df['Entropy_Slope'] = df_with_entropy.get('Composite_Entropy_slope', 0)

        # Transition detection thresholds
        high_accel_threshold = 0.02
        high_slope_threshold = 0.05

        # Detect potential transitions
        transition_df['Transition_Warning'] = False
        transition_df['Transition_Type'] = 'NONE'

        # Rising entropy (moving toward chaos)
        rising_entropy = (
            (transition_df['Entropy_Slope'] > high_slope_threshold) &
            (transition_df['Entropy_Acceleration'] > high_accel_threshold)
        )
        transition_df.loc[rising_entropy, 'Transition_Warning'] = True
        transition_df.loc[rising_entropy, 'Transition_Type'] = 'TO_HIGH_ENTROPY'

        # Falling entropy (moving toward order)
        falling_entropy = (
            (transition_df['Entropy_Slope'] < -high_slope_threshold) &
            (transition_df['Entropy_Acceleration'] < -high_accel_threshold)
        )
        transition_df.loc[falling_entropy, 'Transition_Warning'] = True
        transition_df.loc[falling_entropy, 'Transition_Type'] = 'TO_LOW_ENTROPY'

        # Entropy spike detection
        entropy_volatility = transition_df['Entropy_Slope'].rolling(10).std()
        entropy_spike = abs(transition_df['Entropy_Slope']) > (entropy_volatility * 2)

        transition_df.loc[entropy_spike, 'Transition_Warning'] = True
        transition_df.loc[entropy_spike & (transition_df['Entropy_Slope'] > 0), 'Transition_Type'] = 'ENTROPY_SPIKE_UP'
        transition_df.loc[entropy_spike & (transition_df['Entropy_Slope'] < 0), 'Transition_Type'] = 'ENTROPY_SPIKE_DOWN'

        return transition_df

    def get_entropy_regime_summary(self, df: pd.DataFrame) -> Dict:
        """
        Get comprehensive entropy regime analysis summary
        """
        df_with_regimes = self.detect_regime_history(df)

        if not self.regime_history:
            return {'error': 'No regime history available'}

        df_history = pd.DataFrame(self.regime_history)

        summary = {
            'regime_distribution': df_history['regime'].value_counts(normalize=True).to_dict(),
            'average_confidence': df_history.groupby('regime')['confidence'].mean().to_dict(),
            'entropy_statistics': self._calculate_entropy_statistics(df_history),
            'regime_transitions': self._analyze_entropy_transitions(df_history),
            'current_regime': self.regime_history[-1] if self.regime_history else None
        }

        return summary

    def _calculate_mutual_information(self, series1: pd.Series, series2: pd.Series, window: int) -> pd.Series:
        """
        Calculate rolling mutual information between two series
        """
        try:
            result = []
            for i in range(len(series1)):
                if i < window:
                    result.append(np.nan)
                    continue

                s1 = series1.iloc[i-window:i].dropna()
                s2 = series2.iloc[i-window:i].dropna()

                if len(s1) < 10 or len(s2) < 10:
                    result.append(0)
                    continue

                # Align series (take common indices)
                common_idx = s1.index.intersection(s2.index)
                if len(common_idx) < 10:
                    result.append(0)
                    continue

                s1_aligned = s1[common_idx].values
                s2_aligned = s2[common_idx].values

                # Calculate mutual information
                mi = mutual_info_regression(s1_aligned.reshape(-1, 1), s2_aligned, random_state=42)[0]
                result.append(mi)

            return pd.Series(result, index=series1.index)
        except Exception as e:
            print(f"Warning: Mutual information calculation failed: {e}")
            return pd.Series(0, index=series1.index)

    # Utility methods for entropy calculations

    def _calculate_categorical_entropy(self, series):
        """Calculate entropy for categorical data"""
        try:
            # Handle both pandas Series and numpy arrays
            if hasattr(series, 'dropna'):
                series = series.dropna()
            else:
                # For numpy arrays, remove NaN values
                series = series[~np.isnan(series)] if len(series) > 0 else series

            if len(series) == 0:
                return 0

            # Convert to pandas Series for value_counts
            if not isinstance(series, pd.Series):
                series = pd.Series(series)

            value_counts = series.value_counts(normalize=True)
            entropy = -np.sum(value_counts * np.log2(value_counts + 1e-8))
            return entropy
        except Exception as e:
            return 0

    def _calculate_continuous_entropy(self, series, bins=10):
        """Calculate entropy for continuous data using binning"""
        try:
            # Handle both pandas Series and numpy arrays
            if hasattr(series, 'dropna'):
                series = series.dropna()
            else:
                # For numpy arrays, remove NaN values
                if len(series) > 0:
                    series = series[~np.isnan(series)]
                else:
                    series = series

            if len(series) < 2:
                return 0

            # Convert to numpy array for histogram
            if hasattr(series, 'values'):
                series = series.values

            hist, _ = np.histogram(series, bins=bins)
            hist = hist + 1e-8  # Avoid log(0)
            probs = hist / np.sum(hist)
            entropy = -np.sum(probs * np.log2(probs))
            return entropy
        except Exception as e:
            return 0

    def _calculate_distribution_entropy(self, returns):
        """Calculate entropy of return distribution"""
        try:
            # Handle both pandas Series and numpy arrays
            if hasattr(returns, 'dropna'):
                returns = returns.dropna()
            else:
                # For numpy arrays, remove NaN values
                if len(returns) > 0:
                    returns = returns[~np.isnan(returns)]

            if len(returns) < 10:
                return 0

            # Convert to numpy array for calculations
            if hasattr(returns, 'values'):
                returns = returns.values

            # Use adaptive binning based on data characteristics
            q25, q75 = np.percentile(returns, [25, 75])
            iqr = q75 - q25
            bin_width = 2 * iqr / (len(returns) ** (1/3))

            if bin_width <= 0:
                bins = 10
            else:
                bins = int((returns.max() - returns.min()) / bin_width)
                bins = max(5, min(bins, 20))  # Reasonable range

            return self._calculate_continuous_entropy(returns, bins=bins)
        except Exception as e:
            return 0

    def _calculate_tail_entropy(self, returns):
        """Calculate entropy focusing on tail events"""
        try:
            # Handle both pandas Series and numpy arrays
            if hasattr(returns, 'dropna'):
                returns = returns.dropna()
            else:
                # For numpy arrays, remove NaN values
                if len(returns) > 0:
                    returns = returns[~np.isnan(returns)]

            if len(returns) < 10:
                return 0

            # Convert to numpy array for calculations
            if hasattr(returns, 'values'):
                returns = returns.values

            # Focus on extreme quantiles
            q05 = np.percentile(returns, 5)
            q95 = np.percentile(returns, 95)

            tail_returns = returns[(returns <= q05) | (returns >= q95)]
            if len(tail_returns) < 2:
                return 0

            return self._calculate_continuous_entropy(tail_returns, bins=6)
        except Exception as e:
            return 0

    def _calculate_volume_price_entropy(self, df, window):
        """Calculate entropy of volume distribution across price levels"""
        try:
            result = []
            for i in range(len(df)):
                if i < window:
                    result.append(np.nan)
                    continue

                subset = df.iloc[i-window:i]

                # Handle edge case where all prices are the same
                if subset['Close'].nunique() < 2:
                    result.append(0)
                    continue

                try:
                    price_levels = pd.qcut(subset['Close'], q=6, duplicates='drop')
                    volume_by_level = subset.groupby(price_levels)['Volume'].sum()

                    if len(volume_by_level) < 2:
                        result.append(0)
                    else:
                        probs = volume_by_level / volume_by_level.sum()
                        entropy = -np.sum(probs * np.log2(probs + 1e-8))
                        result.append(entropy)
                except (ValueError, TypeError):
                    # qcut failed due to insufficient unique values
                    result.append(0)

            return pd.Series(result, index=df.index)
        except Exception as e:
            print(f"Warning: Volume-price entropy calculation failed: {e}")
            return pd.Series(0, index=df.index)

    def _calculate_entropy_statistics(self, df_history):
        """Calculate summary statistics for entropy measures"""
        entropy_stats = {}

        entropy_columns = ['direction_entropy', 'magnitude_entropy', 'volume_entropy',
                          'vol_entropy', 'distribution_entropy', 'composite_entropy']

        for col in entropy_columns:
            if col in df_history.columns:
                col_data = df_history[col].dropna()
                if len(col_data) > 0:
                    entropy_stats[col] = {
                        'mean': col_data.mean(),
                        'std': col_data.std(),
                        'min': col_data.min(),
                        'max': col_data.max(),
                        'current': df_history[col].iloc[-1] if len(df_history) > 0 else 0
                    }
                else:
                    entropy_stats[col] = {
                        'mean': 0, 'std': 0, 'min': 0, 'max': 0, 'current': 0
                    }

        return entropy_stats

    def _analyze_entropy_transitions(self, df_history):
        """Analyze regime transitions from entropy perspective"""
        if len(df_history) < 2:
            return {'total_transitions': 0, 'transition_frequency': 0}

        df_history_copy = df_history.copy()
        df_history_copy['prev_regime'] = df_history_copy['regime'].shift(1)
        transitions = df_history_copy[df_history_copy['regime'] != df_history_copy['prev_regime']].dropna()

        if len(transitions) == 0:
            return {
                'total_transitions': 0,
                'transition_frequency': 0,
                'common_transitions': {},
                'average_confidence_at_transition': 0
            }

        transition_analysis = {
            'total_transitions': len(transitions),
            'transition_frequency': len(transitions) / len(df_history),
            'common_transitions': transitions['prev_regime'].value_counts().head().to_dict(),
            'average_confidence_at_transition': transitions['confidence'].mean()
        }

        return transition_analysis
