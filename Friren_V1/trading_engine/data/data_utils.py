import pandas as pd
import numpy as np
from sklearn.feature_selection import mutual_info_regression
from typing import List, Dict, Optional, Union

class StockDataTools:
    """
    Comprehensive data processing and technical analysis toolkit

    This class handles all DataFrame modifications, technical indicators,
    and signal generation. It works with data fetched from StockDataFetcher.
    """

    def __init__(self):
        pass

    # ==================== BASIC TECHNICAL INDICATORS ====================

    def add_moving_averages(self, df, ma_list=[5, 10, 20, 50], ma_types=['SMA']):
        """
        Add moving averages to DataFrame

        Args:
            df: DataFrame with OHLCV data
            ma_list: List of periods for moving averages
            ma_types: List of MA types ('SMA', 'EMA')

        Returns:
            DataFrame: Enhanced with moving averages
        """
        df_ma = df.copy()

        for ma_type in ma_types:
            for period in ma_list:
                column_name = f'{ma_type}_{period}'

                if ma_type == "SMA":
                    df_ma[column_name] = df_ma['Close'].rolling(window=period).mean()
                elif ma_type == 'EMA':
                    df_ma[column_name] = df_ma['Close'].ewm(span=period).mean()
                else:
                    print(f"Warning: Unknown MA type '{ma_type}'. Skipping.")

        return df_ma

    def add_rsi(self, df, period=14):
        """
        Add Relative Strength Index (RSI) to DataFrame

        Args:
            df: DataFrame with OHLCV data
            period: RSI calculation period (typically 14)

        Returns:
            DataFrame: Enhanced with RSI column
        """
        df_rsi = df.copy()

        # Calculate price changes
        delta = df_rsi['Close'].diff()

        # Separate gains and losses
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)

        # Calculate average gains and losses using Wilder's smoothing
        avg_gains = gains.ewm(alpha=1/period, adjust=False).mean()
        avg_losses = losses.ewm(alpha=1/period, adjust=False).mean()

        # Calculate RSI
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))

        df_rsi['RSI'] = rsi
        return df_rsi

    def add_bollinger_bands(self, df, period=20, num_std=2):
        """
        Add Bollinger Bands to DataFrame

        Args:
            df: DataFrame with OHLCV data
            period: Period for moving average and standard deviation
            num_std: Number of standard deviations for bands

        Returns:
            DataFrame: Enhanced with Bollinger Bands
        """
        df_bb = df.copy()

        # Middle band: Simple moving average
        df_bb['MA'] = df_bb['Close'].rolling(window=period).mean()

        # Rolling standard deviation
        df_bb['STD'] = df_bb['Close'].rolling(window=period).std()

        # Upper and lower bands
        df_bb['UpperBand'] = df_bb['MA'] + num_std * df_bb['STD']
        df_bb['LowerBand'] = df_bb['MA'] - num_std * df_bb['STD']

        # Bollinger Band position (0 = lower band, 1 = upper band)
        df_bb['BB_Position'] = (df_bb['Close'] - df_bb['LowerBand']) / (df_bb['UpperBand'] - df_bb['LowerBand'])

        # Bollinger Band width (volatility measure)
        df_bb['BB_Width'] = (df_bb['UpperBand'] - df_bb['LowerBand']) / df_bb['MA']

        return df_bb

    def add_macd(self, df, fast_period=12, slow_period=26, signal_period=9):
        """
        Add MACD (Moving Average Convergence Divergence) to DataFrame

        Args:
            df: DataFrame with OHLCV data
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line EMA period

        Returns:
            DataFrame: Enhanced with MACD indicators
        """
        df_macd = df.copy()

        # Calculate MACD components
        ema_fast = df_macd['Close'].ewm(span=fast_period).mean()
        ema_slow = df_macd['Close'].ewm(span=slow_period).mean()

        df_macd['MACD'] = ema_fast - ema_slow
        df_macd['MACD_Signal'] = df_macd['MACD'].ewm(span=signal_period).mean()
        df_macd['MACD_Histogram'] = df_macd['MACD'] - df_macd['MACD_Signal']

        return df_macd

    def add_stochastic(self, df, k_period=14, d_period=3):
        """
        Add Stochastic Oscillator to DataFrame

        Args:
            df: DataFrame with OHLCV data
            k_period: %K period
            d_period: %D smoothing period

        Returns:
            DataFrame: Enhanced with Stochastic indicators
        """
        df_stoch = df.copy()

        # Calculate %K
        lowest_low = df_stoch['Low'].rolling(window=k_period).min()
        highest_high = df_stoch['High'].rolling(window=k_period).max()

        df_stoch['%K'] = 100 * (df_stoch['Close'] - lowest_low) / (highest_high - lowest_low)
        df_stoch['%D'] = df_stoch['%K'].rolling(window=d_period).mean()

        return df_stoch

    # ==================== VOLATILITY INDICATORS ====================

    def add_atr(self, df, period=14):
        """
        Add Average True Range (ATR) to DataFrame

        Args:
            df: DataFrame with OHLCV data
            period: ATR calculation period

        Returns:
            DataFrame: Enhanced with ATR
        """
        df_atr = df.copy()

        # Calculate True Range components
        high_low = df_atr['High'] - df_atr['Low']
        high_close_prev = np.abs(df_atr['High'] - df_atr['Close'].shift(1))
        low_close_prev = np.abs(df_atr['Low'] - df_atr['Close'].shift(1))

        # True Range is the maximum of the three
        true_range = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))

        # ATR is the smoothed average of True Range
        df_atr[f'ATR_{period}'] = true_range.ewm(alpha=1/period, adjust=False).mean()

        return df_atr

    def add_volatility_features(self, df, windows=[5, 10, 20]):
        """
        Add comprehensive volatility features

        Args:
            df: DataFrame with OHLCV data
            windows: List of periods for rolling calculations

        Returns:
            DataFrame: Enhanced with volatility features
        """
        df_vol = df.copy()

        # Daily returns
        df_vol['Returns'] = df_vol['Close'].pct_change()

        # Rolling volatility (annualized)
        for window in windows:
            df_vol[f'Volatility_{window}'] = df_vol['Returns'].rolling(window).std() * np.sqrt(252)

        # Intraday volatility measures
        df_vol['Intraday_Range'] = (df_vol['High'] - df_vol['Low']) / df_vol['Close']
        df_vol['Open_Close_Range'] = abs(df_vol['Close'] - df_vol['Open']) / df_vol['Close']

        # Add ATR if not already present
        if f'ATR_{windows[0]}' not in df_vol.columns:
            df_vol = self.add_atr(df_vol, period=windows[0])

        return df_vol

    # ==================== REGIME DETECTION INDICATORS ====================

    def add_regime_trend_indicators(self, df):
        """
        Add trend indicators specifically for regime detection

        Args:
            df: DataFrame with OHLCV data

        Returns:
            DataFrame: Enhanced with regime trend indicators
        """
        df_regime = df.copy()

        # Add moving averages if not present - ensure we have all required MAs
        required_mas = [20, 50, 200]
        existing_mas = [int(col.split('_')[1]) for col in df_regime.columns if col.startswith('SMA_')]
        missing_mas = [ma for ma in required_mas if ma not in existing_mas]

        if missing_mas:
            df_regime = self.add_moving_averages(df_regime, ma_list=missing_mas, ma_types=['SMA'])

        # Price relative to moving averages (with safety checks)
        if 'SMA_20' in df_regime.columns:
            df_regime['Price_vs_SMA20'] = (df_regime['Close'] - df_regime['SMA_20']) / df_regime['SMA_20']

        if 'SMA_50' in df_regime.columns:
            df_regime['Price_vs_SMA50'] = (df_regime['Close'] - df_regime['SMA_50']) / df_regime['SMA_50']

            # Only calculate this if we have both SMA_20 and SMA_50
            if 'SMA_20' in df_regime.columns:
                df_regime['SMA20_vs_SMA50'] = (df_regime['SMA_20'] - df_regime['SMA_50']) / df_regime['SMA_50']

        # Long-term trend (if we have 200-day SMA)
        if 'SMA_200' in df_regime.columns and not df_regime['SMA_200'].isna().all():
            df_regime['Price_vs_SMA200'] = (df_regime['Close'] - df_regime['SMA_200']) / df_regime['SMA_200']

        return df_regime

    def add_regime_momentum_indicators(self, df, lookback=10):
        """
        Add momentum indicators for market structure analysis

        Args:
            df: DataFrame with OHLCV data
            lookback: Lookback period for momentum calculations

        Returns:
            DataFrame: Enhanced with momentum indicators
        """
        df_momentum = df.copy()

        # Market structure indicators
        df_momentum['Higher_Highs'] = (df_momentum['High'] > df_momentum['High'].shift(1)).rolling(lookback).sum()
        df_momentum['Lower_Lows'] = (df_momentum['Low'] < df_momentum['Low'].shift(1)).rolling(lookback).sum()

        # Price momentum over different timeframes
        df_momentum['Price_Change_5d'] = df_momentum['Close'].pct_change(5)
        df_momentum['Price_Change_20d'] = df_momentum['Close'].pct_change(20)

        # Momentum score (-1 to +1, where +1 is strong uptrend)
        df_momentum['Momentum_Score'] = (df_momentum['Higher_Highs'] - df_momentum['Lower_Lows']) / lookback

        return df_momentum

    def add_all_regime_indicators(self, df):
        """
        Add comprehensive set of indicators for regime detection

        Args:
            df: DataFrame with OHLCV data

        Returns:
            DataFrame: Enhanced with all regime detection indicators
        """
        print("Adding regime detection indicators...")

        # Add basic technical indicators
        df_enhanced = self.add_rsi(df, period=14)
        df_enhanced = self.add_bollinger_bands(df_enhanced, period=20, num_std=2)
        df_enhanced = self.add_macd(df_enhanced)

        # Add regime-specific indicators
        df_enhanced = self.add_regime_trend_indicators(df_enhanced)
        df_enhanced = self.add_volatility_features(df_enhanced)
        df_enhanced = self.add_regime_momentum_indicators(df_enhanced)

        print(f"Added regime indicators to {len(df_enhanced)} rows")
        return df_enhanced

    # ==================== HELPER METHODS FOR SAFE OPERATIONS ====================

    def safe_set_signal(self, df, bar_idx, column, value, delay_bars=1):
        """
        Helper method for safe signal setting with execution delay

        Args:
            df: DataFrame to modify
            bar_idx: Current bar index
            column: Column to set signal in
            value: Signal value to set
            delay_bars: Number of bars to delay execution

        Returns:
            bool: True if signal was set successfully
        """
        target_idx = bar_idx + delay_bars
        if target_idx < len(df):
            df.at[df.index[target_idx], column] = value
            return True
        return False

    def safe_get_value(self, df, bar_idx, column, default=np.nan):
        """
        Safely get value from DataFrame

        Args:
            df: DataFrame to read from
            bar_idx: Row index
            column: Column name
            default: Default value if not found

        Returns:
            Value from DataFrame or default
        """
        try:
            if bar_idx < len(df):
                return df.iloc[bar_idx][column]
            return default
        except (KeyError, IndexError):
            return default

    def initialize_signal_columns(self, df, columns):
        """
        Initialize signal columns safely

        Args:
            df: DataFrame to modify
            columns: List of column names to initialize

        Returns:
            DataFrame: DataFrame with initialized columns
        """
        df_copy = df.copy()
        for col in columns:
            if col not in df_copy.columns:
                df_copy[col] = 0
        return df_copy

    # ==================== MEAN REVERSION STRATEGIES ====================

    def add_zscore_signals(self, df, lookback=20, entry_threshold=2.0, exit_threshold=0.5):
        """
        Add Z-Score mean reversion signals with realistic execution timing

        Args:
            df: DataFrame with OHLCV data
            lookback: Rolling window for mean/std calculation
            entry_threshold: Z-score threshold for entry (absolute value)
            exit_threshold: Z-score threshold for exit (absolute value)

        Returns:
            DataFrame: Enhanced with Z-score signals
        """
        df_zscore = df.copy()

        # Calculate rolling statistics
        df_zscore['Price_Mean'] = df_zscore['Close'].rolling(window=lookback).mean()
        df_zscore['Price_Std'] = df_zscore['Close'].rolling(window=lookback).std()
        df_zscore['Z_Score'] = (df_zscore['Close'] - df_zscore['Price_Mean']) / df_zscore['Price_Std']

        # Initialize signal columns
        df_zscore = self.initialize_signal_columns(df_zscore, ['ZScore_Signal', 'ZScore_Position'])

        position = 0

        # Generate signals with execution delay
        for i in range(lookback, len(df_zscore) - 1):
            z_score = self.safe_get_value(df_zscore, i, 'Z_Score')

            if pd.isna(z_score):
                continue

            if position == 0:  # No position
                if z_score > entry_threshold:  # Overbought - short signal
                    if self.safe_set_signal(df_zscore, i, 'ZScore_Signal', -1):
                        position = -1
                elif z_score < -entry_threshold:  # Oversold - long signal
                    if self.safe_set_signal(df_zscore, i, 'ZScore_Signal', 1):
                        position = 1
            else:  # Have position
                if abs(z_score) < exit_threshold:  # Exit signal
                    if self.safe_set_signal(df_zscore, i, 'ZScore_Signal', -position):
                        position = 0

            # Position tracking (current state)
            df_zscore.at[df_zscore.index[i], 'ZScore_Position'] = position

        return df_zscore

    def add_pairs_trading_signals(self, df1, df2, symbol1, symbol2, lookback=60, entry_threshold=2.0):
        """
        Add pairs trading signals with realistic execution timing

        Args:
            df1, df2: DataFrames for the two assets
            symbol1, symbol2: Symbol names for column naming
            lookback: Window for calculating spread statistics
            entry_threshold: Standard deviations for entry signal

        Returns:
            DataFrame: Pairs trading signals
        """
        # Align dataframes
        common_dates = df1.index.intersection(df2.index)
        df1_aligned = df1.loc[common_dates].copy()
        df2_aligned = df2.loc[common_dates].copy()

        # Calculate spread
        spread = np.log(df1_aligned['Close']) - np.log(df2_aligned['Close'])
        spread_mean = spread.rolling(window=lookback).mean()
        spread_std = spread.rolling(window=lookback).std()
        spread_zscore = (spread - spread_mean) / spread_std

        # Create signals DataFrame
        pairs_signals = pd.DataFrame(index=common_dates)
        pairs_signals['Spread'] = spread
        pairs_signals['Spread_ZScore'] = spread_zscore
        pairs_signals[f'{symbol1}_Signal'] = 0
        pairs_signals[f'{symbol2}_Signal'] = 0

        # Generate signals with execution delay
        for i in range(lookback, len(pairs_signals) - 1):
            zscore = pairs_signals.iloc[i]['Spread_ZScore']

            if pd.isna(zscore):
                continue

            # Signals execute next bar
            next_idx = i + 1
            if next_idx < len(pairs_signals):
                if zscore < -entry_threshold:  # Long spread
                    pairs_signals.iloc[next_idx, pairs_signals.columns.get_loc(f'{symbol1}_Signal')] = 1
                    pairs_signals.iloc[next_idx, pairs_signals.columns.get_loc(f'{symbol2}_Signal')] = -1
                elif zscore > entry_threshold:  # Short spread
                    pairs_signals.iloc[next_idx, pairs_signals.columns.get_loc(f'{symbol1}_Signal')] = -1
                    pairs_signals.iloc[next_idx, pairs_signals.columns.get_loc(f'{symbol2}_Signal')] = 1
                elif abs(zscore) < 0.5:  # Exit signal
                    pairs_signals.iloc[next_idx, pairs_signals.columns.get_loc(f'{symbol1}_Signal')] = 0
                    pairs_signals.iloc[next_idx, pairs_signals.columns.get_loc(f'{symbol2}_Signal')] = 0

        return pairs_signals

    def add_reversion_strength_indicator(self, df, short_window=5, long_window=20):
        """
        Measure mean reversion strength for regime-aware trading

        Args:
            df: DataFrame with OHLCV data
            short_window: Short-term window
            long_window: Long-term window

        Returns:
            DataFrame: Enhanced with reversion strength indicators
        """
        df_reversion = df.copy()

        # Short-term and long-term returns
        df_reversion['Return_Short'] = df_reversion['Close'].pct_change(short_window)
        df_reversion['Return_Long'] = df_reversion['Close'].pct_change(long_window)

        # Reversion strength: negative correlation between short and long returns
        df_reversion['Reversion_Strength'] = df_reversion['Return_Short'].rolling(window=long_window).corr(
            df_reversion['Return_Long'].shift(short_window)
        )

        # Volatility-adjusted reversion
        df_reversion['Return_Vol'] = df_reversion['Close'].pct_change().rolling(window=long_window).std()
        df_reversion['Reversion_Score'] = df_reversion['Reversion_Strength'] / (df_reversion['Return_Vol'] + 1e-8)

        return df_reversion

    # ==================== MOMENTUM/BREAKOUT STRATEGIES ====================

    def add_momentum_signals(self, df, short_ma=10, long_ma=50, momentum_threshold=0.02):
        """
        Add multi-timeframe momentum signals with realistic execution timing

        Args:
            df: DataFrame with OHLCV data
            short_ma: Short moving average period
            long_ma: Long moving average period
            momentum_threshold: Minimum momentum for signal generation

        Returns:
            DataFrame: Enhanced with momentum signals
        """
        df_momentum = df.copy()

        # Add moving averages if not present
        if f'SMA_{short_ma}' not in df_momentum.columns:
            df_momentum = self.add_moving_averages(df_momentum, ma_list=[short_ma, long_ma], ma_types=['SMA'])

        # Calculate momentum indicators
        df_momentum['Price_Momentum'] = df_momentum['Close'].pct_change(short_ma)
        df_momentum['MA_Momentum'] = (df_momentum[f'SMA_{short_ma}'] - df_momentum[f'SMA_{long_ma}']) / df_momentum[f'SMA_{long_ma}']
        df_momentum['Volume_Momentum'] = df_momentum['Volume'].pct_change(5)
        df_momentum['Trend_Strength'] = abs(df_momentum['MA_Momentum'])
        df_momentum['Price_vs_MA'] = (df_momentum['Close'] - df_momentum[f'SMA_{short_ma}']) / df_momentum[f'SMA_{short_ma}']

        # Initialize signal column
        df_momentum = self.initialize_signal_columns(df_momentum, ['Momentum_Signal'])

        # Generate signals with execution delay
        for i in range(max(short_ma, long_ma), len(df_momentum) - 1):
            # Get current bar values
            sma_short = self.safe_get_value(df_momentum, i, f'SMA_{short_ma}')
            sma_long = self.safe_get_value(df_momentum, i, f'SMA_{long_ma}')
            price_momentum = self.safe_get_value(df_momentum, i, 'Price_Momentum')
            volume_momentum = self.safe_get_value(df_momentum, i, 'Volume_Momentum')
            close_price = self.safe_get_value(df_momentum, i, 'Close')

            # Skip if any required data is missing
            if any(pd.isna(x) for x in [sma_short, sma_long, price_momentum, volume_momentum, close_price]):
                continue

            # Long condition
            long_condition = (
                (sma_short > sma_long) and  # Uptrend
                (price_momentum > momentum_threshold) and  # Strong momentum
                (volume_momentum > 0) and  # Volume confirmation
                (close_price > sma_short)  # Price above short MA
            )

            # Short condition
            short_condition = (
                (sma_short < sma_long) and  # Downtrend
                (price_momentum < -momentum_threshold) and  # Strong negative momentum
                (volume_momentum > 0) and  # Volume confirmation
                (close_price < sma_short)  # Price below short MA
            )

            # Set signals for next bar execution
            if long_condition:
                self.safe_set_signal(df_momentum, i, 'Momentum_Signal', 1)
            elif short_condition:
                self.safe_set_signal(df_momentum, i, 'Momentum_Signal', -1)

        return df_momentum

    def add_breakout_signals(self, df, lookback=20, volume_multiplier=1.5, atr_multiplier=2.0):
        """
        Add volume-confirmed breakout signals with realistic execution timing

        Args:
            df: DataFrame with OHLCV data
            lookback: Lookback period for support/resistance
            volume_multiplier: Volume confirmation multiplier
            atr_multiplier: ATR-based breakout confirmation

        Returns:
            DataFrame: Enhanced with breakout signals
        """
        df_breakout = df.copy()

        # Calculate support/resistance levels
        df_breakout['Resistance'] = df_breakout['High'].rolling(window=lookback).max()
        df_breakout['Support'] = df_breakout['Low'].rolling(window=lookback).min()
        df_breakout['Range_Middle'] = (df_breakout['Resistance'] + df_breakout['Support']) / 2
        df_breakout['Avg_Volume'] = df_breakout['Volume'].rolling(window=lookback).mean()
        df_breakout['Volume_Ratio'] = df_breakout['Volume'] / df_breakout['Avg_Volume']

        # Add ATR if not present
        if 'ATR_20' not in df_breakout.columns:
            df_breakout = self.add_atr(df_breakout, period=20)

        # Initialize signal columns
        df_breakout = self.initialize_signal_columns(df_breakout, ['Breakout_Signal'])

        # Generate signals with execution delay
        for i in range(lookback, len(df_breakout) - 1):
            # Get current values
            close_price = self.safe_get_value(df_breakout, i, 'Close')
            resistance = self.safe_get_value(df_breakout, i-1, 'Resistance')  # Previous resistance
            support = self.safe_get_value(df_breakout, i-1, 'Support')       # Previous support
            volume_ratio = self.safe_get_value(df_breakout, i, 'Volume_Ratio')
            high_price = self.safe_get_value(df_breakout, i, 'High')
            low_price = self.safe_get_value(df_breakout, i, 'Low')
            open_price = self.safe_get_value(df_breakout, i, 'Open')
            atr = self.safe_get_value(df_breakout, i, 'ATR_20')

            # Skip if missing data
            if any(pd.isna(x) for x in [close_price, resistance, support, volume_ratio, atr]):
                continue

            # Upside breakout condition
            upside_breakout = (
                (close_price > resistance) and
                (volume_ratio > volume_multiplier) and
                (high_price - open_price > atr_multiplier * atr)
            )

            # Downside breakout condition
            downside_breakout = (
                (close_price < support) and
                (volume_ratio > volume_multiplier) and
                (open_price - low_price > atr_multiplier * atr)
            )

            # Set signals for next bar execution
            if upside_breakout:
                self.safe_set_signal(df_breakout, i, 'Breakout_Signal', 1)
            elif downside_breakout:
                self.safe_set_signal(df_breakout, i, 'Breakout_Signal', -1)

        return df_breakout

    # ==================== VOLATILITY STRATEGIES ====================

    def add_volatility_regime_signals(self, df, vol_window=20, regime_threshold=0.6):
        """
        Add volatility regime-based trading signals

        Args:
            df: DataFrame with OHLCV data
            vol_window: Window for volatility calculation
            regime_threshold: Threshold for regime classification

        Returns:
            DataFrame: Enhanced with volatility regime signals
        """
        df_vol = df.copy()

        # Add volatility indicators if not present
        if 'Volatility_20' not in df_vol.columns:
            df_vol = self.add_volatility_features(df_vol, windows=[vol_window])

        # Calculate volatility percentiles
        df_vol['Vol_Percentile'] = df_vol[f'Volatility_{vol_window}'].rolling(window=126).rank(pct=True)

        # Classify volatility regimes
        df_vol['Vol_Regime'] = 'Medium'
        df_vol.loc[df_vol['Vol_Percentile'] > regime_threshold, 'Vol_Regime'] = 'High'
        df_vol.loc[df_vol['Vol_Percentile'] < (1 - regime_threshold), 'Vol_Regime'] = 'Low'

        # Initialize signal column
        df_vol = self.initialize_signal_columns(df_vol, ['Vol_Strategy_Signal'])

        # Add Z-score for high vol regime (if not present)
        if 'Z_Score' not in df_vol.columns:
            df_vol['Price_Mean'] = df_vol['Close'].rolling(window=20).mean()
            df_vol['Price_Std'] = df_vol['Close'].rolling(window=20).std()
            df_vol['Z_Score'] = (df_vol['Close'] - df_vol['Price_Mean']) / df_vol['Price_Std']

        # Add momentum signals for low vol regime (if not present)
        if 'MA_Momentum' not in df_vol.columns:
            if 'SMA_10' not in df_vol.columns:
                df_vol = self.add_moving_averages(df_vol, ma_list=[10, 50], ma_types=['SMA'])
            df_vol['MA_Momentum'] = (df_vol['SMA_10'] - df_vol['SMA_50']) / df_vol['SMA_50']

        # Generate regime-based signals
        for i in range(vol_window, len(df_vol) - 1):
            vol_regime = self.safe_get_value(df_vol, i, 'Vol_Regime')
            z_score = self.safe_get_value(df_vol, i, 'Z_Score')
            ma_momentum = self.safe_get_value(df_vol, i, 'MA_Momentum')

            if pd.isna(z_score) or pd.isna(ma_momentum):
                continue

            # High volatility regime: mean reversion
            if vol_regime == 'High':
                if z_score > 1.2:
                    self.safe_set_signal(df_vol, i, 'Vol_Strategy_Signal', -1)
                elif z_score < -1.2:
                    self.safe_set_signal(df_vol, i, 'Vol_Strategy_Signal', 1)

            # Low volatility regime: momentum
            elif vol_regime == 'Low':
                if ma_momentum > 0.015:
                    self.safe_set_signal(df_vol, i, 'Vol_Strategy_Signal', 1)
                elif ma_momentum < -0.015:
                    self.safe_set_signal(df_vol, i, 'Vol_Strategy_Signal', -1)

        return df_vol

    # ==================== COMPOSITE SIGNALS ====================

    def add_composite_signals(self, df, weights=None):
        """
        Combine multiple strategy signals with realistic execution timing

        Args:
            df: DataFrame with multiple signal columns
            weights: Dictionary of signal weights

        Returns:
            DataFrame: Enhanced with composite signals
        """
        df_composite = df.copy()

        # Default weights
        if weights is None:
            weights = {
                'Momentum_Signal': 0.2,
                'ZScore_Signal': 0.2,
                'Breakout_Signal': 0.15,
                'Vol_Strategy_Signal': 0.15,
                'RSI_Signal': 0.1,
                'MACD_Signal': 0.1,
                'BB_Signal': 0.1
            }

        # Initialize composite columns
        df_composite['Composite_Signal'] = 0.0
        df_composite['Final_Signal'] = 0
        df_composite['Signal_Strength'] = 0.0

        # Calculate composite signals
        for i in range(len(df_composite)):
            composite_score = 0.0

            # Sum weighted signals from current bar
            for signal_col, weight in weights.items():
                if signal_col in df_composite.columns:
                    signal_value = self.safe_get_value(df_composite, i, signal_col, 0)
                    if not pd.isna(signal_value):
                        composite_score += float(signal_value) * weight

            # Set composite score
            df_composite.iloc[i, df_composite.columns.get_loc('Composite_Signal')] = composite_score

            # Generate final signal
            if composite_score > 0.3:
                final_signal = 1
            elif composite_score < -0.3:
                final_signal = -1
            else:
                final_signal = 0

            df_composite.iloc[i, df_composite.columns.get_loc('Final_Signal')] = final_signal
            df_composite.iloc[i, df_composite.columns.get_loc('Signal_Strength')] = abs(composite_score)

        return df_composite

    # ==================== UTILITY METHODS ====================

    def calculate_signal_metrics(self, df, signal_column='Final_Signal', price_column='Close'):
        """
        Calculate performance metrics for any signal

        Args:
            df: DataFrame with signals and prices
            signal_column: Name of signal column
            price_column: Name of price column

        Returns:
            dict: Performance metrics
        """
        signals = df[signal_column].dropna()
        prices = df[price_column].dropna()

        # Align signals and prices
        common_index = signals.index.intersection(prices.index)
        signals_aligned = signals.loc[common_index]
        prices_aligned = prices.loc[common_index]

        # Calculate forward returns for each signal
        forward_returns = prices_aligned.pct_change().shift(-1)  # Next period return

        # Signal performance
        long_signals = signals_aligned == 1
        short_signals = signals_aligned == -1

        if long_signals.any():
            long_returns = forward_returns[long_signals]
            avg_long_return = long_returns.mean()
            long_hit_rate = (long_returns > 0).mean()
        else:
            avg_long_return, long_hit_rate = 0, 0

        if short_signals.any():
            short_returns = -forward_returns[short_signals]  # Invert for short positions
            avg_short_return = short_returns.mean()
            short_hit_rate = (short_returns > 0).mean()
        else:
            avg_short_return, short_hit_rate = 0, 0

        return {
            'long_avg_return': avg_long_return,
            'short_avg_return': avg_short_return,
            'long_hit_rate': long_hit_rate,
            'short_hit_rate': short_hit_rate,
            'total_signals': len(signals_aligned[signals_aligned != 0]),
            'long_signals': long_signals.sum(),
            'short_signals': short_signals.sum()
        }

    def add_all_indicators(self, df):
        """
        Add comprehensive set of technical indicators

        Args:
            df: DataFrame with OHLCV data

        Returns:
            DataFrame: Enhanced with all indicators
        """
        print("Adding comprehensive technical indicators...")

        # Basic indicators
        df_enhanced = self.add_moving_averages(df, ma_list=[5, 10, 20, 50, 200], ma_types=['SMA', 'EMA'])
        df_enhanced = self.add_rsi(df_enhanced, period=14)
        df_enhanced = self.add_bollinger_bands(df_enhanced, period=20, num_std=2)
        df_enhanced = self.add_macd(df_enhanced)
        df_enhanced = self.add_stochastic(df_enhanced)
        df_enhanced = self.add_atr(df_enhanced, period=14)

        # Volatility and regime indicators
        df_enhanced = self.add_volatility_features(df_enhanced)
        df_enhanced = self.add_regime_trend_indicators(df_enhanced)
        df_enhanced = self.add_regime_momentum_indicators(df_enhanced)

        print(f"Added comprehensive indicators to {len(df_enhanced)} rows")
        return df_enhanced



if __name__ == "__main__":
    # Test the data tools with sample data
    from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData

    # Fetch some sample data
    fetcher = YahooFinancePriceData()
    sample_data = fetcher.extract_data('AAPL', period='6mo')

    if not sample_data.empty:
        # Initialize tools
        tools = StockDataTools()

        # Test basic indicators
        print("Testing basic indicators...")
        enhanced_data = tools.add_moving_averages(sample_data)
        enhanced_data = tools.add_rsi(enhanced_data)
        enhanced_data = tools.add_bollinger_bands(enhanced_data)

        print(f"Enhanced data shape: {enhanced_data.shape}")
        print(f"New columns: {[col for col in enhanced_data.columns if col not in sample_data.columns]}")

        # Test signal generation
        print("\nTesting signal generation...")
        signal_data = tools.add_zscore_signals(enhanced_data)
        signal_data = tools.add_momentum_signals(signal_data)

        # Calculate signal metrics
        metrics = tools.calculate_signal_metrics(signal_data, 'ZScore_Signal')
        print(f"Z-Score signal metrics: {metrics}")

        print("\nStockDataTools testing completed successfully.")
    else:
        print("Could not fetch sample data for testing.")
