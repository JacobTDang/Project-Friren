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

      if stock_data:
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
    return self.get_multiple_stocks(major_funds_symbols)

  def add_moving_averages(self, df ,ma_list=[5,10,20,50], ma_types=['SMA']):
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
          df_ma[column_name] = df_ma['Close'].rolling(window=mAvg).mean()

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
