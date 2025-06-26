import sys
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Friren_V1.trading_engine.data.data_utils import StockDataTools

@dataclass
class RegimeConfig:
    """Configuration for regime detection parameters"""
    # Trend thresholds (percentiles instead of hardcoded)
    trend_percentile_threshold: float = 0.2  # 20th/80th percentiles
    volatility_percentile_threshold: float = 0.25  # 25th/75th percentiles

    # Moving average periods
    trend_periods: List[int] = None
    volatility_window: int = 20
    momentum_lookback: int = 10

    # ADX parameters for trend strength
    adx_period: int = 14
    adx_trend_threshold: float = 25  # ADX > 25 indicates trending

    # Bollinger Band Width for sideways detection
    bb_period: int = 20
    bb_std: float = 2.0
    bb_width_threshold_percentile: float = 0.3  # 30th percentile

    # Cross-market signals
    use_vix_signals: bool = True
    use_bond_signals: bool = False  # For future implementation

    # Lookback periods for percentile calculations
    percentile_lookback: int = 252  # 1 year

    def __post_init__(self):
        if self.trend_periods is None:
            self.trend_periods = [20, 50, 200]

class EnhancedRegimeDetector:
    """
    Professional-grade regime detector with all fixes applied
    """

    def __init__(self, config: Optional[RegimeConfig] = None):
        self.config = config or RegimeConfig()
        self.SDT = StockDataTools()
        self.regime_history = []

    def add_enhanced_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add all enhanced indicators for regime detection
        """
        df = df.copy()

        # Basic indicators from StockDataTools
        df = self.SDT.add_all_regime_indicators(df)

        # Add ADX for trend strength
        df = self._add_adx(df, self.config.adx_period)

        # Add Bollinger Band Width for sideways detection
        df = self._add_bb_width(df, self.config.bb_period, self.config.bb_std)

        # Add percentile-based thresholds
        df = self._add_percentile_thresholds(df)

        return df

    def _add_adx(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        FIXED: Add Average Directional Index (ADX) for trend strength measurement
        """
        df = df.copy()

        # Calculate True Range
        high_low = df['High'] - df['Low']
        high_close_prev = np.abs(df['High'] - df['Close'].shift(1))
        low_close_prev = np.abs(df['Low'] - df['Close'].shift(1))
        tr = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))

        # Calculate Directional Movement
        up_move = df['High'] - df['High'].shift(1)
        down_move = df['Low'].shift(1) - df['Low']

        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

        # Smooth the values using Wilder's smoothing (exponential)
        alpha = 1.0 / period

        # Initialize first values
        tr_smooth = tr.ewm(alpha=alpha, adjust=False).mean()
        plus_dm_smooth = pd.Series(plus_dm).ewm(alpha=alpha, adjust=False).mean()
        minus_dm_smooth = pd.Series(minus_dm).ewm(alpha=alpha, adjust=False).mean()

        # Calculate DI+ and DI-
        plus_di = 100 * (plus_dm_smooth / tr_smooth)
        minus_di = 100 * (minus_dm_smooth / tr_smooth)

        # Calculate DX and ADX
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        # Replace inf and nan values
        dx = dx.replace([np.inf, -np.inf], 0).fillna(0)

        adx = dx.ewm(alpha=alpha, adjust=False).mean()

        df['ADX'] = adx
        df['Plus_DI'] = plus_di
        df['Minus_DI'] = minus_di

        return df

    def _add_bb_width(self, df: pd.DataFrame, period: int = 20, num_std: float = 2.0) -> pd.DataFrame:
        """
        Add Bollinger Band Width for sideways market detection
        """
        df = df.copy()

        # Calculate Bollinger Bands if not already present
        if 'UpperBand' not in df.columns:
            df = self.SDT.add_bollinger_bands(df, period=period, num_std=num_std)

        # Calculate BB Width as percentage of middle band
        df['BB_Width'] = (df['UpperBand'] - df['LowerBand']) / df['MA']
        df['BB_Width_Pct'] = df['BB_Width'] * 100  # Express as percentage

        return df

    def _add_percentile_thresholds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        FIXED: Add percentile-based thresholds with better handling
        """
        df = df.copy()
        lookback = min(self.config.percentile_lookback, len(df) - 1)

        # FIXED: Reduced minimum data requirement
        if lookback < 30:  # Reduced from 50
            return df

        # Trend percentiles
        trend_pct = self.config.trend_percentile_threshold

        # Only calculate if we have enough data
        if len(df['Price_vs_SMA20'].dropna()) >= 30:
            df['Price_vs_SMA20_Upper'] = df['Price_vs_SMA20'].rolling(lookback, min_periods=30).quantile(1 - trend_pct)
            df['Price_vs_SMA20_Lower'] = df['Price_vs_SMA20'].rolling(lookback, min_periods=30).quantile(trend_pct)

        if len(df['SMA20_vs_SMA50'].dropna()) >= 30:
            df['SMA20_vs_SMA50_Upper'] = df['SMA20_vs_SMA50'].rolling(lookback, min_periods=30).quantile(1 - trend_pct)
            df['SMA20_vs_SMA50_Lower'] = df['SMA20_vs_SMA50'].rolling(lookback, min_periods=30).quantile(trend_pct)

        # Volatility percentiles
        vol_pct = self.config.volatility_percentile_threshold
        if len(df['Volatility_20'].dropna()) >= 30:
            df['Volatility_Upper'] = df['Volatility_20'].rolling(lookback, min_periods=30).quantile(1 - vol_pct)
            df['Volatility_Lower'] = df['Volatility_20'].rolling(lookback, min_periods=30).quantile(vol_pct)

        # BB Width percentiles
        bb_pct = self.config.bb_width_threshold_percentile
        if len(df['BB_Width'].dropna()) >= 30:
            df['BB_Width_Threshold'] = df['BB_Width'].rolling(lookback, min_periods=30).quantile(bb_pct)

        return df

    def detect_regime_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        FIXED: TIME-AWARE regime detection with better handling
        """
        # Add all enhanced indicators
        df_enhanced = self.add_enhanced_indicators(df)

        # Initialize regime columns
        df_enhanced['Regime'] = 'UNKNOWN'
        df_enhanced['Regime_Confidence'] = 0.0
        df_enhanced['Regime_Scores'] = None

        # FIXED: More reasonable start index
        start_idx = max(30, len(df_enhanced) // 4)  # Start after 25% of data or 30 days minimum

        regime_history = []

        for i in range(start_idx, len(df_enhanced)):
            # Get subset for regime detection - use more data for better percentiles
            lookback = min(100, i)  # Use up to 100 days of history
            subset = df_enhanced.iloc[i-lookback:i+1]

            # Detect regime for this point in time
            regime_info = self._detect_single_regime(subset)

            # Store in dataframe
            df_enhanced.iloc[i, df_enhanced.columns.get_loc('Regime')] = regime_info['regime']
            df_enhanced.iloc[i, df_enhanced.columns.get_loc('Regime_Confidence')] = regime_info['confidence']
            df_enhanced.iloc[i, df_enhanced.columns.get_loc('Regime_Scores')] = str(regime_info['scores'])

            # Store in history
            regime_history.append({
                'date': df_enhanced.index[i],
                'regime': regime_info['regime'],
                'confidence': regime_info['confidence'],
                **regime_info['indicators']
            })

        self.regime_history = regime_history
        return df_enhanced

    def detect_regime(self, df: pd.DataFrame) -> Dict:
        """
        Detect current regime (latest point only)
        """
        df_enhanced = self.add_enhanced_indicators(df)
        return self._detect_single_regime(df_enhanced)

    def _detect_single_regime(self, df: pd.DataFrame) -> Dict:
        """
        FIXED: Enhanced regime detection with better confidence scoring
        """
        # FIXED: Reduced minimum data requirement
        if len(df) < 20:
            return {
                'regime': 'UNKNOWN',
                'confidence': 0.0,
                'scores': {},
                'indicators': {}
            }

        latest = df.iloc[-1]

        # Initialize regime scores
        regime_scores = {
            'BULL_MARKET': 0,
            'BEAR_MARKET': 0,
            'SIDEWAYS': 0,
            'HIGH_VOLATILITY': 0,
            'LOW_VOLATILITY': 0
        }

        # 1. TREND ANALYSIS - More lenient thresholds
        price_vs_sma20 = latest.get('Price_vs_SMA20', 0)
        sma20_vs_sma50 = latest.get('SMA20_vs_SMA50', 0)

        # Use fixed thresholds if percentiles not available (more lenient)
        price_upper = latest.get('Price_vs_SMA20_Upper', 0.015)  # Reduced from 0.02
        price_lower = latest.get('Price_vs_SMA20_Lower', -0.015)  # Reduced from -0.02
        sma_upper = latest.get('SMA20_vs_SMA50_Upper', 0.008)   # Reduced from 0.01
        sma_lower = latest.get('SMA20_vs_SMA50_Lower', -0.008)  # Reduced from -0.01

        if price_vs_sma20 > price_upper:
            regime_scores['BULL_MARKET'] += 2  # Increased weight
        elif price_vs_sma20 < price_lower:
            regime_scores['BEAR_MARKET'] += 2  # Increased weight
        else:
            regime_scores['SIDEWAYS'] += 2

        if sma20_vs_sma50 > sma_upper:
            regime_scores['BULL_MARKET'] += 2
        elif sma20_vs_sma50 < sma_lower:
            regime_scores['BEAR_MARKET'] += 2
        else:
            regime_scores['SIDEWAYS'] += 2

        # 2. RSI + ADX COMBINATION (Fixed ADX handling)
        rsi = latest.get('RSI', 50)
        adx = latest.get('ADX', 0)

        # Handle NaN ADX values
        if pd.isna(adx):
            adx = 0

        if adx > self.config.adx_trend_threshold:
            # Strong trend detected by ADX
            if rsi > 60:  # Lowered from 65
                regime_scores['BULL_MARKET'] += 2
            elif rsi < 40:  # Raised from 35
                regime_scores['BEAR_MARKET'] += 2
        else:
            # Weak trend or sideways market
            if rsi > 65:  # Lowered from 70
                regime_scores['SIDEWAYS'] += 1
            elif rsi < 35:  # Raised from 30
                regime_scores['SIDEWAYS'] += 1

        # 3. EXPLICIT SIDEWAYS DETECTION (More sensitive)
        bb_width = latest.get('BB_Width', 0)
        bb_threshold = latest.get('BB_Width_Threshold', np.nan)

        if not pd.isna(bb_width):
            # Use fixed threshold if percentile not available
            if pd.isna(bb_threshold):
                bb_threshold = 0.08  # 8% BB width threshold

            if bb_width < bb_threshold:
                regime_scores['SIDEWAYS'] += 3  # Strong sideways signal

        # ATR-based confirmation
        atr_20 = latest.get('ATR_20', 0)
        current_price = latest.get('Close', 100)
        if atr_20 > 0 and current_price > 0:
            atr_pct = atr_20 / current_price
            if atr_pct < 0.025:  # Increased from 0.02
                regime_scores['SIDEWAYS'] += 2

        # 4. VOLATILITY ANALYSIS
        volatility = latest.get('Volatility_20', 0.15)
        vol_upper = latest.get('Volatility_Upper', 0.30)  # Increased threshold
        vol_lower = latest.get('Volatility_Lower', 0.10)  # Decreased threshold

        if volatility > vol_upper:
            regime_scores['HIGH_VOLATILITY'] += 3
        elif volatility < vol_lower:
            regime_scores['LOW_VOLATILITY'] += 3

        # 5. CROSS-MARKET SIGNALS (VIX Integration)
        if self.config.use_vix_signals:
            vix_signal = self._get_vix_regime_signal()
            if vix_signal == 'HIGH_VOLATILITY':
                regime_scores['HIGH_VOLATILITY'] += 1
            elif vix_signal == 'LOW_VOLATILITY':
                regime_scores['LOW_VOLATILITY'] += 1

        # 6. MOMENTUM ANALYSIS (More sensitive)
        momentum_score = latest.get('Momentum_Score', 0)
        if momentum_score > 0.2:  # Lowered from 0.3
            regime_scores['BULL_MARKET'] += 1
        elif momentum_score < -0.2:  # Raised from -0.3
            regime_scores['BEAR_MARKET'] += 1

        # 7. LONG-TERM TREND (200-day MA) - More lenient
        price_vs_sma200 = latest.get('Price_vs_SMA200', 0)
        if not pd.isna(price_vs_sma200):
            if price_vs_sma200 > 0.03:  # Lowered from 0.05
                regime_scores['BULL_MARKET'] += 1
            elif price_vs_sma200 < -0.03:  # Raised from -0.05
                regime_scores['BEAR_MARKET'] += 1

        # 8. DETERMINE PRIMARY REGIME
        # Volatility regimes take precedence
        if regime_scores['HIGH_VOLATILITY'] >= 3:
            primary_regime = 'HIGH_VOLATILITY'
        elif regime_scores['LOW_VOLATILITY'] >= 3:
            primary_regime = 'LOW_VOLATILITY'
        else:
            # Choose highest scoring trend regime
            trend_scores = {
                'BULL_MARKET': regime_scores['BULL_MARKET'],
                'BEAR_MARKET': regime_scores['BEAR_MARKET'],
                'SIDEWAYS': regime_scores['SIDEWAYS']
            }
            primary_regime = max(trend_scores, key=trend_scores.get)

        # IMPROVED CONFIDENCE CALCULATION
        max_score = regime_scores[primary_regime]
        total_possible = 12.0  # Adjusted for new scoring

        # Confidence based on score ratio and margin
        confidence = min(max_score / total_possible, 1.0)

        # Boost confidence if there's a clear winner
        sorted_scores = sorted(regime_scores.values(), reverse=True)
        if len(sorted_scores) >= 2:
            margin = sorted_scores[0] - sorted_scores[1]
            confidence = min(confidence + (margin / total_possible), 1.0)

        return {
            'regime': primary_regime,
            'confidence': confidence,
            'scores': regime_scores,
            'indicators': {
                'price_vs_sma20': price_vs_sma20,
                'sma20_vs_sma50': sma20_vs_sma50,
                'volatility': volatility,
                'rsi': rsi,
                'adx': adx,
                'bb_width': bb_width,
                'momentum_score': momentum_score,
                'price_vs_sma200': price_vs_sma200
            }
        }

    def _get_vix_regime_signal(self) -> str:
        """
        Get VIX-based regime signal for cross-market confirmation
        """
        try:
            # Fetch VIX data
            vix_data = self.SDT.extract_data("^VIX", period="1y", interval="1d")

            if len(vix_data) < 50:
                return 'NEUTRAL'

            current_vix = vix_data['Close'].iloc[-1]
            vix_percentile = (vix_data['Close'].rank(pct=True).iloc[-1])

            if vix_percentile > 0.8:  # 80th percentile
                return 'HIGH_VOLATILITY'
            elif vix_percentile < 0.2:  # 20th percentile
                return 'LOW_VOLATILITY'
            else:
                return 'NEUTRAL'

        except Exception as e:
            print(f"Warning: Could not fetch VIX data: {e}")
            return 'NEUTRAL'

    def analyze_regime_transitions(self) -> pd.DataFrame:
        """
        Analyze regime transitions over time
        """
        if not self.regime_history:
            print("No regime history available. Run detect_regime_history first.")
            return pd.DataFrame()

        df_history = pd.DataFrame(self.regime_history)
        df_history['prev_regime'] = df_history['regime'].shift(1)

        # Identify transitions
        transitions = df_history[df_history['regime'] != df_history['prev_regime']].copy()
        transitions = transitions.dropna()

        # Add transition type
        transitions['transition'] = transitions['prev_regime'] + ' → ' + transitions['regime']

        return transitions

    def get_regime_statistics(self) -> Dict:
        """
        Get comprehensive regime statistics
        """
        if not self.regime_history:
            return {}

        df_history = pd.DataFrame(self.regime_history)

        stats = {
            'regime_distribution': df_history['regime'].value_counts(normalize=True).to_dict(),
            'average_confidence': df_history.groupby('regime')['confidence'].mean().to_dict(),
            'regime_persistence': self._calculate_regime_persistence(df_history),
            'transition_matrix': self._calculate_transition_matrix(df_history)
        }

        return stats

    def _calculate_regime_persistence(self, df_history: pd.DataFrame) -> Dict:
        """Calculate how long each regime typically lasts"""
        persistence = {}

        for regime in df_history['regime'].unique():
            regime_runs = []
            current_run = 0

            for r in df_history['regime']:
                if r == regime:
                    current_run += 1
                else:
                    if current_run > 0:
                        regime_runs.append(current_run)
                    current_run = 0

            if current_run > 0:
                regime_runs.append(current_run)

            persistence[regime] = {
                'avg_duration': np.mean(regime_runs) if regime_runs else 0,
                'max_duration': np.max(regime_runs) if regime_runs else 0
            }

        return persistence

    def _calculate_transition_matrix(self, df_history: pd.DataFrame) -> Dict:
        """Calculate regime transition probabilities"""
        df_history['prev_regime'] = df_history['regime'].shift(1)
        transitions = df_history.dropna()

        matrix = {}
        for from_regime in transitions['prev_regime'].unique():
            matrix[from_regime] = {}
            subset = transitions[transitions['prev_regime'] == from_regime]
            total = len(subset)

            for to_regime in subset['regime'].unique():
                count = len(subset[subset['regime'] == to_regime])
                matrix[from_regime][to_regime] = count / total

        return matrix

