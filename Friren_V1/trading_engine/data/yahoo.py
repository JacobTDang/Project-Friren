import yfinance as yf
import pandas as pd
import numpy as np

msft = yf.Ticker("MSFT")

# functions to build
# save_to_database, load_from_database - to do later (avoid headache)
# get_live_price, stream_prices - maybe for later on HFT implementation (that is like v2)

class StockDataTools:
    """
    Fetch data with yahoo finance api
    """

    def __init__(self):
        pass

    def extract_data(self, symbol, period="1y", interval = "1d"):
        """
        Extract data from selected symbol, period, interval
        returns stock data with ohlcv columns
        """

        ticker = yf.Ticker(symbol)
        # get historical data
        df = ticker.history(period=period, interval=interval)

        # get current data

        current_info = ticker.info
        current_price = current_info.get('currentPrice', current_info.get('regularMarketPrice'))
        df.attrs['current_price'] = current_price

        return df

    def get_multiple_stocks(self, symbol_list, period="1y", interval= "1d"):
        """
        get data from multiple stocks
        returns a dict of stock with its info {"symbol_name" : df info}
        """
        data = {}
        failed_symbols = []

        for symbol in symbol_list:
            print(f'Fetching data from {symbol}')

            stock_data = self.extract_data(symbol, period, interval)

            if stock_data is not None and not stock_data.empty:
                data[symbol] = stock_data
            else:
                failed_symbols.append(symbol)

        return data

    def get_top_stocks(self, period="1y", interval= "1d"):
        """
        extracs top s&p 500 stocks
        returns a dict of stock with its info {"symbol_name" : df info}
        """

        # pls spare my hardcoded stuff
        major_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
            'UNH', 'XOM', 'JNJ', 'JPM', 'V', 'PG', 'MA', 'CVX', 'HD', 'PFE',
            'ABBV', 'BAC', 'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'WMT', 'DIS',
            'ABT', 'CRM', 'ACN', 'VZ', 'ADBE', 'DHR', 'TXN', 'NEE', 'NKE',
            'LIN', 'RTX', 'QCOM', 'MCD', 'UPS', 'PM', 'T', 'HON', 'AMGN',
            'LOW', 'COP', 'IBM', 'ELV'
        ]

        print(f"fetching your hardcoded code mf: {major_symbols}")
        return self.get_multiple_stocks(major_symbols, period, interval)

    def get_market_data(self, period="1y", interval= "1d"):
        """
        used to see what the conditions of the market is like by
        returning from major funds
        """
        major_funds_symbols = ['SPY', '^VIX', 'QQQ']
        return self.get_multiple_stocks(major_funds_symbols, period, interval)

    def add_moving_averages(self, df, ma_list=[5,10,20,50], ma_types=['SMA']):
        """
        adds moving averages to df
        copies and returns a df with ma from ma_list and ma_types
        simple moving average or exponential moving average
        return modifed dataframe with applied effects
        """

        df_ma = df.copy()
        for type in ma_types:
            for mAvg in ma_list:
                column_name = f'{type}_{mAvg}'
                if type == "SMA":
                    # access the close column and apply rolling window of mAvg
                    df_ma[column_name] = df_ma['Close'].rolling(window=mAvg).mean()
                elif type == 'EMA':
                    df_ma[column_name] = df_ma['Close'].ewm(span=mAvg).mean()

        return df_ma

    def add_rsi(self, df, period=14):
        """
        adds relative strength index to df
        period is in days and is typically 14
        """
        df_rsi = df.copy()

        # get the price difference
        delta = df_rsi['Close'].diff()

        # seperate out the gains and losses
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0,0)

        """
        -----------------------Visualizer-------------------------
        delta:    [ 0.5, -0.2, 0.3, -0.1 ] # differences
        gains:    [ 0.5,  0.0, 0.3,  0.0 ] # replace losses w/ 0
        losses:   [ 0.0,  0.2, 0.0,  0.1 ] # replace wins w/ 0
        ----------------------------------------------------------
        """

        # calculate average gain + losses
        avg_gains = gains.rolling(window=period).mean()
        avg_losses = losses.rolling(window=period).mean()

        # calculate RSI
        relative_strength = avg_gains / avg_losses
        rsi = 100 - (100/ (1 + relative_strength))

        df_rsi['RSI'] = rsi

        return df_rsi

    def add_bollinger_bands(self,df, period=20, num_std=2):
        """
        given moving average over n days and standard deivation
        upper band = MA + k * sd
        lower band = MA - k * sd
        """

        df_bb = df.copy()

        # Middle band: simple moving average
        df_bb['MA'] = df_bb['Close'].rolling(window=period).mean()

        # Rolling standard deviation
        df_bb['STD'] = df_bb['Close'].rolling(window=period).std()

        # Upper and lower bands
        df_bb['UpperBand'] = df_bb['MA'] + num_std * df_bb['STD']
        df_bb['LowerBand'] = df_bb['MA'] - num_std * df_bb['STD']

        return df_bb

    def validate_data(self, df, symbol):
        """
        runs through a bunch of different test to make sure data is legit
        returns t or f if it is valid data
        """
        issues = []

        # basic structure checks
        if df.empty:
            return False, ["DataFrame is empty"]

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, [f"Missing required columns: {missing_cols}"]

        # price validation
        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            if (df[col] <= 0).any():
                issues.append(f"Invalid prices in {col}")

        # OHLC logic validation
        if (df['High'] < df['Low']).any():
            issues.append("High < Low violations")

        # extreme moves
        daily_returns = df['Close'].pct_change().abs()
        if (daily_returns > 0.5).any():
            extreme_count = (daily_returns > 0.5).sum()
            issues.append(f"{extreme_count} days with >50% price moves")

        # volume checks
        if (df['Volume'] < 0).any():
            issues.append("Negative volume detected")

        # data completeness
        if df.isnull().any().any():
            issues.append("Missing values detected")

        # return results
        is_valid = len(issues) == 0
        if not is_valid:
            print(f"Data validation failed for {symbol}:")
            for issue in issues:
                print(f"  - {issue}")

        return is_valid, issues

    def add_volatility_features(self, df, windows=[5, 10, 20]):
        """
        Ok im tired, it is like 1:56 am, claude did this bc he is the goat
        Add volatility-based features for regime detection

        Args:
            df: DataFrame with OHLC data
            windows: List of periods for rolling calculations

        Returns:
            DataFrame: Enhanced with volatility features
        """
        df_vol = df.copy()

        # 1. Calculate daily returns first
        df_vol['daily_return'] = df_vol['Close'].pct_change()

        # 2. Rolling volatility (standard deviation of returns)
        for window in windows:
            df_vol[f'volatility_{window}d'] = df_vol['daily_return'].rolling(window).std()

        # 3. Intraday volatility (High-Low range)
        df_vol['intraday_range'] = (df_vol['High'] - df_vol['Low']) / df_vol['Close']

        # 4. True Range (more sophisticated volatility measure)
        df_vol['true_range'] = self._calculate_true_range(df_vol)

        return df_vol

    def _calculate_true_range(self, df):
        """Calculate True Range for ATR"""
        high_low = df['High'] - df['Low']
        high_close_prev = abs(df['High'] - df['Close'].shift(1))
        low_close_prev = abs(df['Low'] - df['Close'].shift(1))

        return pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)

    def add_regime_trend_indicators(self, df):
        """
        Add trend indicators for regime detection
        """
        df = df.copy()

        # Use your existing moving average method
        df = self.add_moving_averages(df, ma_list=[20, 50, 200], ma_types=['SMA'])

        # Add regime-specific trend metrics
        df['Price_vs_SMA20'] = (df['Close'] - df['SMA_20']) / df['SMA_20']
        df['Price_vs_SMA50'] = (df['Close'] - df['SMA_50']) / df['SMA_50']
        df['SMA20_vs_SMA50'] = (df['SMA_20'] - df['SMA_50']) / df['SMA_50']

        # Long-term trend (if we have 200-day SMA)
        if 'SMA_200' in df.columns and not df['SMA_200'].isna().all():
            df['Price_vs_SMA200'] = (df['Close'] - df['SMA_200']) / df['SMA_200']

        return df

    def add_regime_volatility_indicators(self, df, window=20):
        """
        Add volatility indicators for regime detection
        """
        df = df.copy()

        # Returns
        df['Returns'] = df['Close'].pct_change()

        # Rolling volatility (annualized)
        df[f'Volatility_{window}'] = df['Returns'].rolling(window).std() * np.sqrt(252)

        # ATR (Average True Range)
        high_low = df['High'] - df['Low']
        high_close_prev = np.abs(df['High'] - df['Close'].shift(1))
        low_close_prev = np.abs(df['Low'] - df['Close'].shift(1))
        true_range = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))
        df[f'ATR_{window}'] = true_range.rolling(window).mean()

        return df

    def add_regime_momentum_indicators(self, df, lookback=10):
        """
        Add momentum indicators for market structure analysis
        """
        df = df.copy()

        # Market structure
        df['Higher_Highs'] = (df['High'] > df['High'].shift(1)).rolling(lookback).sum()
        df['Lower_Lows'] = (df['Low'] < df['Low'].shift(1)).rolling(lookback).sum()

        # Price momentum
        df['Price_Change_5d'] = df['Close'].pct_change(5)
        df['Price_Change_20d'] = df['Close'].pct_change(20)

        # Momentum score (-1 to +1, where +1 is strong uptrend)
        df['Momentum_Score'] = (df['Higher_Highs'] - df['Lower_Lows']) / lookback

        return df

    def add_all_regime_indicators(self, df):
        """
        Add all indicators needed for regime detection
        This is the main method you'll use
        """
        print("Adding regime detection indicators...")

        # Add basic indicators (your existing methods)
        df = self.add_rsi(df, period=14)
        df = self.add_bollinger_bands(df, period=20, num_std=2)

        # Add regime-specific indicators
        df = self.add_regime_trend_indicators(df)
        df = self.add_regime_volatility_indicators(df)
        df = self.add_regime_momentum_indicators(df)

        print(f"Added regime indicators to {len(df)} rows")
        return df

    # ==================== HELPER METHODS FOR SAFE OPERATIONS ====================

    def safe_set_signal(self, df, bar_idx, column, value, delay_bars=1):
        """Helper method for safe signal setting with execution delay"""
        target_idx = bar_idx + delay_bars
        if target_idx < len(df):
            df.at[df.index[target_idx], column] = value
            return True
        return False

    def safe_get_value(self, df, bar_idx, column, default=np.nan):
        """Safely get value from DataFrame"""
        try:
            if bar_idx < len(df):
                return df.iloc[bar_idx][column]
            return default
        except (KeyError, IndexError):
            return default

    def initialize_signal_columns(self, df, columns):
        """Initialize signal columns safely"""
        df_copy = df.copy()
        for col in columns:
            if col not in df_copy.columns:
                df_copy[col] = 0
        return df_copy

    # ==================== MEAN REVERSION STRATEGIES ====================

    def add_zscore_signals(self, df, lookback=20, entry_threshold=2.0, exit_threshold=0.5):
        """
        Z-Score mean reversion signals with realistic execution timing
        Args:
            df: DataFrame with OHLCV data
            lookback: Rolling window for mean/std calculation
            entry_threshold: Z-score threshold for entry (absolute value)
            exit_threshold: Z-score threshold for exit (absolute value)
        """
        df = df.copy()

        # Calculate rolling statistics
        df['Price_Mean'] = df['Close'].rolling(window=lookback).mean()
        df['Price_Std'] = df['Close'].rolling(window=lookback).std()
        df['Z_Score'] = (df['Close'] - df['Price_Mean']) / df['Price_Std']

        # Initialize signal columns
        df = self.initialize_signal_columns(df, ['ZScore_Signal', 'ZScore_Position'])

        position = 0

        # Stop at len(df)-1 to prevent index out of bounds
        for i in range(lookback, len(df) - 1):
            z_score = self.safe_get_value(df, i, 'Z_Score')

            if pd.isna(z_score):
                continue

            if position == 0:  # No position
                if z_score > entry_threshold:  # Overbought - short signal
                    if self.safe_set_signal(df, i, 'ZScore_Signal', -1):
                        position = -1
                elif z_score < -entry_threshold:  # Oversold - long signal
                    if self.safe_set_signal(df, i, 'ZScore_Signal', 1):
                        position = 1
            else:  # Have position
                if abs(z_score) < exit_threshold:  # Exit signal
                    if self.safe_set_signal(df, i, 'ZScore_Signal', -position):
                        position = 0

            # Position tracking (current state)
            df.at[df.index[i], 'ZScore_Position'] = position

        return df

    def add_pairs_trading_signals(self, df1, df2, symbol1, symbol2, lookback=60, entry_threshold=2.0):
        """
        Pairs trading signals with realistic execution timing
        Args:
            df1, df2: DataFrames for the two assets
            symbol1, symbol2: Symbol names for column naming
            lookback: Window for calculating spread statistics
            entry_threshold: Standard deviations for entry signal
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
        """
        df = df.copy()

        # Short-term and long-term returns
        df['Return_Short'] = df['Close'].pct_change(short_window)
        df['Return_Long'] = df['Close'].pct_change(long_window)

        # Reversion strength: negative correlation between short and long returns
        df['Reversion_Strength'] = df['Return_Short'].rolling(window=long_window).corr(
            df['Return_Long'].shift(short_window)
        )

        # Volatility-adjusted reversion
        df['Return_Vol'] = df['Close'].pct_change().rolling(window=long_window).std()
        df['Reversion_Score'] = df['Reversion_Strength'] / (df['Return_Vol'] + 1e-8)

        return df

    # ==================== MOMENTUM/BREAKOUT STRATEGIES ====================

    def add_momentum_signals(self, df, short_ma=10, long_ma=50, momentum_threshold=0.02):
        """
        Multi-timeframe momentum signals with realistic execution timing
        """
        df = df.copy()

        # Add moving averages if not present
        if f'SMA_{short_ma}' not in df.columns:
            df = self.add_moving_averages(df, ma_list=[short_ma, long_ma], ma_types=['SMA'])

        # Calculate momentum indicators
        df['Price_Momentum'] = df['Close'].pct_change(short_ma)
        df['MA_Momentum'] = (df[f'SMA_{short_ma}'] - df[f'SMA_{long_ma}']) / df[f'SMA_{long_ma}']
        df['Volume_Momentum'] = df['Volume'].pct_change(5)
        df['Trend_Strength'] = abs(df['MA_Momentum'])
        df['Price_vs_MA'] = (df['Close'] - df[f'SMA_{short_ma}']) / df[f'SMA_{short_ma}']

        # Initialize signal column
        df = self.initialize_signal_columns(df, ['Momentum_Signal'])

        # Generate signals with execution delay
        for i in range(max(short_ma, long_ma), len(df) - 1):
            # Get current bar values
            sma_short = self.safe_get_value(df, i, f'SMA_{short_ma}')
            sma_long = self.safe_get_value(df, i, f'SMA_{long_ma}')
            price_momentum = self.safe_get_value(df, i, 'Price_Momentum')
            volume_momentum = self.safe_get_value(df, i, 'Volume_Momentum')
            close_price = self.safe_get_value(df, i, 'Close')

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

            #  Set signals for next bar execution
            if long_condition:
                self.safe_set_signal(df, i, 'Momentum_Signal', 1)
            elif short_condition:
                self.safe_set_signal(df, i, 'Momentum_Signal', -1)

        return df

    def add_breakout_signals(self, df, lookback=20, volume_multiplier=1.5, atr_multiplier=2.0):
        """
        Volume-confirmed breakout signals with realistic execution timing
        """
        df = df.copy()

        # Calculate support/resistance levels
        df['Resistance'] = df['High'].rolling(window=lookback).max()
        df['Support'] = df['Low'].rolling(window=lookback).min()
        df['Range_Middle'] = (df['Resistance'] + df['Support']) / 2
        df['Avg_Volume'] = df['Volume'].rolling(window=lookback).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Avg_Volume']

        # Add ATR if not present
        if 'ATR_20' not in df.columns:
            df = self.add_regime_volatility_indicators(df)

        # Initialize signal columns
        df = self.initialize_signal_columns(df, ['Breakout_Signal'])

        # Generate signals with execution delay
        for i in range(lookback, len(df) - 1):
            # Get current values
            close_price = self.safe_get_value(df, i, 'Close')
            resistance = self.safe_get_value(df, i-1, 'Resistance')  # Previous resistance
            support = self.safe_get_value(df, i-1, 'Support')       # Previous support
            volume_ratio = self.safe_get_value(df, i, 'Volume_Ratio')
            high_price = self.safe_get_value(df, i, 'High')
            low_price = self.safe_get_value(df, i, 'Low')
            open_price = self.safe_get_value(df, i, 'Open')
            atr = self.safe_get_value(df, i, 'ATR_20')

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
                self.safe_set_signal(df, i, 'Breakout_Signal', 1)
            elif downside_breakout:
                self.safe_set_signal(df, i, 'Breakout_Signal', -1)

        return df

    def add_opening_range_signals(self, df, range_minutes=30, breakout_threshold=0.5):
        """
        Opening range breakout signals (works best with intraday data)
        For daily data, uses first hour proxy
        """
        df = df.copy()

        # For daily data, approximate opening range with gap analysis
        df['Gap'] = (df['Open'] - df['Close'].shift(1)) / df['Close'].shift(1)
        df['First_Hour_Range'] = abs(df['Gap'])  # Proxy for opening volatility

        # Opening range levels (using daily OHLC as proxy)
        df['OR_High'] = df['Open'] + (df['High'] - df['Low']) * 0.3  # Approximate
        df['OR_Low'] = df['Open'] - (df['High'] - df['Low']) * 0.3

        # Breakout signals
        df['OR_Upside_Break'] = (df['Close'] > df['OR_High']) & (df['Volume'] > df['Volume'].rolling(10).mean())
        df['OR_Downside_Break'] = (df['Close'] < df['OR_Low']) & (df['Volume'] > df['Volume'].rolling(10).mean())

        df['OR_Signal'] = 0
        df.loc[df['OR_Upside_Break'], 'OR_Signal'] = 1
        df.loc[df['OR_Downside_Break'], 'OR_Signal'] = -1

        return df

    # ==================== VOLATILITY STRATEGIES ====================

    def add_volatility_regime_signals_v2(self, df, vol_window=20, regime_threshold=0.6):
        """
        sensitive volatility regime-based trading signals
        """
        df = df.copy()

        # Add volatility indicators if not present
        if 'Volatility_20' not in df.columns:
            df = self.add_regime_volatility_indicators(df, window=vol_window)

        # IMPROVED: More sensitive percentile calculation (shorter window)
        df['Vol_Percentile'] = df['Volatility_20'].rolling(window=126).rank(pct=True)  # 6 months instead of 1 year

        # IMPROVED: More granular regime classification
        df['Vol_Regime'] = 'Medium'
        df.loc[df['Vol_Percentile'] > regime_threshold, 'Vol_Regime'] = 'High'
        df.loc[df['Vol_Percentile'] < (1 - regime_threshold), 'Vol_Regime'] = 'Low'

        # Initialize signal column
        df = self.initialize_signal_columns(df, ['Vol_Strategy_Signal'])

        # Add Z-score for high vol regime (if not present)
        if 'Z_Score' not in df.columns:
            df['Price_Mean'] = df['Close'].rolling(window=20).mean()
            df['Price_Std'] = df['Close'].rolling(window=20).std()
            df['Z_Score'] = (df['Close'] - df['Price_Mean']) / df['Price_Std']

        # Add momentum signals for low vol regime (if not present)
        if 'MA_Momentum' not in df.columns:
            if 'SMA_10' not in df.columns:
                df = self.add_moving_averages(df, ma_list=[10, 50], ma_types=['SMA'])
            df['MA_Momentum'] = (df['SMA_10'] - df['SMA_50']) / df['SMA_50']

        # IMPROVED: More sensitive thresholds
        for i in range(vol_window, len(df) - 1):
            vol_regime = self.safe_get_value(df, i, 'Vol_Regime')
            z_score = self.safe_get_value(df, i, 'Z_Score')
            ma_momentum = self.safe_get_value(df, i, 'MA_Momentum')
            vol_percentile = self.safe_get_value(df, i, 'Vol_Percentile')

            if pd.isna(z_score) or pd.isna(ma_momentum):
                continue

            # High volatility regime: mean reversion (LOWERED thresholds)
            if vol_regime == 'High':
                if z_score > 1.2:  # Was 1.5
                    self.safe_set_signal(df, i, 'Vol_Strategy_Signal', -1)
                elif z_score < -1.2:  # Was -1.5
                    self.safe_set_signal(df, i, 'Vol_Strategy_Signal', 1)

            # Low volatility regime: momentum (LOWERED thresholds)
            elif vol_regime == 'Low':
                if ma_momentum > 0.015:  # Was 0.02
                    self.safe_set_signal(df, i, 'Vol_Strategy_Signal', 1)
                elif ma_momentum < -0.015:  # Was -0.02
                    self.safe_set_signal(df, i, 'Vol_Strategy_Signal', -1)

        return df

    def add_vix_trading_signals(self, df, vix_data=None, mean_reversion_threshold=20):
        """
        VIX-based market timing signals
        Note: Requires VIX data to be passed separately
        """
        df = df.copy()

        if vix_data is not None:
            # Align VIX data with stock data
            common_dates = df.index.intersection(vix_data.index)
            df_aligned = df.loc[common_dates].copy()
            vix_aligned = vix_data.loc[common_dates]['Close']

            # VIX analysis
            df_aligned['VIX'] = vix_aligned
            df_aligned['VIX_MA'] = df_aligned['VIX'].rolling(window=20).mean()
            df_aligned['VIX_Zscore'] = (df_aligned['VIX'] - df_aligned['VIX_MA']) / df_aligned['VIX'].rolling(window=20).std()

            # VIX trading signals
            df_aligned['VIX_Signal'] = 0

            # High VIX (fear) -> potential buying opportunity
            df_aligned.loc[df_aligned['VIX'] > mean_reversion_threshold, 'VIX_Signal'] = 1

            # VIX spike reversion
            df_aligned.loc[df_aligned['VIX_Zscore'] > 2, 'VIX_Signal'] = 1
            df_aligned.loc[df_aligned['VIX_Zscore'] < -1, 'VIX_Signal'] = -1

            return df_aligned
        else:
            # Create placeholder columns
            df['VIX_Signal'] = 0
            print("Warning: No VIX data provided for VIX trading signals")
            return df

    # ==================== EVENT-DRIVEN STRATEGIES ====================

    def add_earnings_momentum_signals(self, df, earnings_dates=None, sentiment_scores=None):
        """
        Pre/post earnings momentum signals with sentiment
        """
        df = df.copy()

        # Create earnings proximity indicator
        df['Days_To_Earnings'] = np.inf
        df['Earnings_Period'] = False

        if earnings_dates is not None:
            for earnings_date in earnings_dates:
                # Calculate days to earnings
                days_diff = (df.index - pd.to_datetime(earnings_date)).days

                # Mark pre-earnings period (5 days before to 2 days after)
                earnings_window = (days_diff >= -5) & (days_diff <= 2)
                df.loc[earnings_window, 'Earnings_Period'] = True

                # Update days to earnings
                mask = abs(days_diff) < abs(df['Days_To_Earnings'])
                df.loc[mask, 'Days_To_Earnings'] = days_diff[mask]

        # Add sentiment if available
        if sentiment_scores is not None:
            df['Sentiment_Score'] = sentiment_scores
        else:
            df['Sentiment_Score'] = 0

        # Generate earnings signals
        df['Earnings_Signal'] = 0

        # Pre-earnings momentum with positive sentiment
        pre_earnings = (df['Days_To_Earnings'] >= -3) & (df['Days_To_Earnings'] <= -1)
        positive_sentiment = df['Sentiment_Score'] > 0.1

        df.loc[pre_earnings & positive_sentiment, 'Earnings_Signal'] = 1
        df.loc[pre_earnings & (df['Sentiment_Score'] < -0.1), 'Earnings_Signal'] = -1

        return df

    def add_news_momentum_signals(self, df, sentiment_scores, sentiment_threshold=0.3, volume_confirm=True):
        """
        News-driven momentum signals with realistic execution timing
        """
        df = df.copy()

        # Add sentiment scores
        if isinstance(sentiment_scores, pd.Series):
            df['News_Sentiment'] = sentiment_scores.reindex(df.index, fill_value=0)
        else:
            df['News_Sentiment'] = sentiment_scores

        # Calculate sentiment indicators
        df['Sentiment_Change'] = df['News_Sentiment'].diff()
        df['Sentiment_MA'] = df['News_Sentiment'].rolling(window=5).mean()

        # Volume confirmation
        if volume_confirm:
            df['Volume_Spike'] = df['Volume'] > df['Volume'].rolling(window=10).mean() * 1.2
        else:
            df['Volume_Spike'] = True

        # Initialize signal column
        df = self.initialize_signal_columns(df, ['News_Signal'])

        # Generate signals with execution delay
        for i in range(10, len(df) - 1):  # Start after volume window
            sentiment = self.safe_get_value(df, i, 'News_Sentiment')
            sentiment_change = self.safe_get_value(df, i, 'Sentiment_Change')
            volume_spike = self.safe_get_value(df, i, 'Volume_Spike')

            if pd.isna(sentiment) or pd.isna(sentiment_change):
                continue

            # Bullish news condition
            bullish_news = (
                (sentiment > sentiment_threshold) and
                (sentiment_change > 0.1) and
                volume_spike
            )

            # Bearish news condition
            bearish_news = (
                (sentiment < -sentiment_threshold) and
                (sentiment_change < -0.1) and
                volume_spike
            )

            # Set signals for next bar execution
            if bullish_news:
                self.safe_set_signal(df, i, 'News_Signal', 1)
            elif bearish_news:
                self.safe_set_signal(df, i, 'News_Signal', -1)

        return df

    # ==================== MULTI-ASSET STRATEGIES ====================

    def add_sector_rotation_signals(self, sector_etfs_data, strength_lookback=20):
        """
        Sector rotation based on relative strength
        Args:
            sector_etfs_data: Dict of sector ETF DataFrames {'XLF': df, 'XLK': df, ...}
            strength_lookback: Period for calculating relative strength
        """
        if not sector_etfs_data:
            return {}

        # Calculate relative strength for each sector
        sector_signals = {}

        # Get common dates
        all_dates = None
        for sector, df in sector_etfs_data.items():
            if all_dates is None:
                all_dates = df.index
            else:
                all_dates = all_dates.intersection(df.index)

        # Calculate relative performance
        sector_returns = {}
        for sector, df in sector_etfs_data.items():
            df_aligned = df.loc[all_dates].copy()
            sector_returns[sector] = df_aligned['Close'].pct_change(strength_lookback)

        returns_df = pd.DataFrame(sector_returns, index=all_dates)

        # Rank sectors by performance
        sector_ranks = returns_df.rank(axis=1, ascending=False)

        # Generate rotation signals
        for sector in sector_etfs_data.keys():
            signals_df = sector_etfs_data[sector].loc[all_dates].copy()
            signals_df['Sector_Rank'] = sector_ranks[sector]
            signals_df['Relative_Strength'] = returns_df[sector]

            # Signal generation: buy top quartile, sell bottom quartile
            total_sectors = len(sector_etfs_data)
            signals_df['Rotation_Signal'] = 0
            signals_df.loc[signals_df['Sector_Rank'] <= total_sectors * 0.25, 'Rotation_Signal'] = 1
            signals_df.loc[signals_df['Sector_Rank'] >= total_sectors * 0.75, 'Rotation_Signal'] = -1

            sector_signals[sector] = signals_df

        return sector_signals

    def add_cross_asset_signals(self, equity_df, bond_df=None, commodity_df=None, risk_on_threshold=0.6):
        """
        Cross-asset momentum and flight-to-quality signals
        """
        equity_df = equity_df.copy()

        # Calculate equity momentum
        equity_df['Equity_Momentum'] = equity_df['Close'].pct_change(10)

        if bond_df is not None:
            # Align bond data
            common_dates = equity_df.index.intersection(bond_df.index)
            equity_aligned = equity_df.loc[common_dates].copy()
            bond_aligned = bond_df.loc[common_dates]

            # Bond momentum (inverse relationship often)
            equity_aligned['Bond_Price'] = bond_aligned['Close']
            equity_aligned['Bond_Momentum'] = equity_aligned['Bond_Price'].pct_change(10)

            # Risk-on/risk-off signal
            equity_aligned['Risk_Sentiment'] = (
                equity_aligned['Equity_Momentum'].rolling(window=5).mean() -
                equity_aligned['Bond_Momentum'].rolling(window=5).mean()
            )

            # Normalize to 0-1 scale
            equity_aligned['Risk_On_Score'] = (
                equity_aligned['Risk_Sentiment'].rolling(window=60).rank(pct=True)
            )

            # Generate cross-asset signals
            equity_aligned['Cross_Asset_Signal'] = 0
            equity_aligned.loc[equity_aligned['Risk_On_Score'] > risk_on_threshold, 'Cross_Asset_Signal'] = 1
            equity_aligned.loc[equity_aligned['Risk_On_Score'] < (1 - risk_on_threshold), 'Cross_Asset_Signal'] = -1

            return equity_aligned
        else:
            equity_df['Cross_Asset_Signal'] = 0
            return equity_df

    # ==================== COMPOSITE SIGNALS ====================

    def add_composite_signals(self, df, weights=None):
      """
      Combine multiple strategy signals with realistic execution timing
      """
      df = df.copy()

      # Default weights
      if weights is None:
          weights = {
              'Momentum_Signal': 0.2,
              'ZScore_Signal': 0.2,
              'Breakout_Signal': 0.15,
              'Vol_Strategy_Signal': 0.15,
              'News_Signal': 0.1,
              'VIX_Signal': 0.1,
              'Cross_Asset_Signal': 0.1
          }

      #  Initialize composite columns with proper dtypes
      df['Composite_Signal'] = 0.0  # Explicit float
      df['Final_Signal'] = 0        # Explicit int
      df['Signal_Strength'] = 0.0   # Explicit float

      # Calculate composite signals with proper timing
      for i in range(len(df)):
          composite_score = 0.0

          # Sum weighted signals from current bar
          for signal_col, weight in weights.items():
              if signal_col in df.columns:
                  signal_value = self.safe_get_value(df, i, signal_col, 0)
                  if not pd.isna(signal_value):
                      composite_score += float(signal_value) * weight  # Explicit float cast

          # Set composite score (now compatible dtypes)
          df.iloc[i, df.columns.get_loc('Composite_Signal')] = composite_score

          # Generate final signal
          if composite_score > 0.3:
              final_signal = 1
          elif composite_score < -0.3:
              final_signal = -1
          else:
              final_signal = 0

          df.iloc[i, df.columns.get_loc('Final_Signal')] = final_signal
          df.iloc[i, df.columns.get_loc('Signal_Strength')] = abs(composite_score)

      return df
    # ==================== UTILITY METHODS ====================

    def calculate_signal_metrics(self, df, signal_column='Final_Signal', price_column='Close'):
        """
        Calculate performance metrics for any signal
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


