import pandas as pd
import numpy as np
from trading_engine.data.yahoo import StockDataTools

class bb_backtest:
  def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.shares = 0
        self.portfolio_value = []
        self.trades = []
        self.SDT = StockDataTools()

  def bollinger_strategy(self, df):
    """
    Use the SDT to create the bollinger band strategy
    """
    df_bb = self.SDT.add_bollinger_bands(df)

    # Initialize signals
    df_bb['signal'] = 0  # 0 = hold, 1 = buy, -1 = sell
    df_bb['position'] = 0  # Track current position

    # Calculate additional indicators for better signals
    df_bb['bb_position'] = (df_bb['Close'] - df_bb['LowerBand']) / (df_bb['UpperBand'] - df_bb['LowerBand'])
    df_bb['bb_width'] = (df_bb['UpperBand'] - df_bb['LowerBand']) / df_bb['MA']
    df_bb['price_vs_ma'] = df_bb['Close'] / df_bb['MA']

    current_position = 0

    for i in range(len(df_bb)):
        if pd.isna(df_bb.iloc[i]['UpperBand']):
            continue

        current_price = df_bb.iloc[i]['Close']
        upper_band = df_bb.iloc[i]['UpperBand']
        lower_band = df_bb.iloc[i]['LowerBand']
        middle_band = df_bb.iloc[i]['MA']
        bb_position = df_bb.iloc[i]['bb_position']
        bb_width = df_bb.iloc[i]['bb_width']

        # Buy Signal: Price touches or goes below lower band
        if current_price <= lower_band and current_position == 0:
            df_bb.iloc[i, df_bb.columns.get_loc('signal')] = 1
            current_position = 1

        # Sell Signal: Price touches or goes above upper band
        elif current_price >= upper_band and current_position == 1:
            df_bb.iloc[i, df_bb.columns.get_loc('signal')] = -1
            current_position = 0

        # Alternative exit: Return to middle band (more conservative)
        elif current_price >= middle_band and current_position == 1:
            df_bb.iloc[i, df_bb.columns.get_loc('signal')] = -1
            current_position = 0

        # Track position
        df_bb.iloc[i, df_bb.columns.get_loc('position')] = current_position

    return df_bb

  def backtest_bollinger_strategy(self, df, period=20, num_std=2):
        """
        Backtest Bollinger Bands mean reversion strategy
        Buy when price touches lower band, sell when price touches upper band
        """
        # Add Bollinger Bands to data
        df = df.copy()
        df['MA'] = df['Close'].rolling(window=period).mean()
        df['STD'] = df['Close'].rolling(window=period).std()
        df['UpperBand'] = df['MA'] + (num_std * df['STD'])
        df['LowerBand'] = df['MA'] - (num_std * df['STD'])

        # Track position
        position = 0  # 0 = no position, 1 = long position

        for i, row in df.iterrows():
            current_price = row['Close']
            current_date = row.name if hasattr(row.name, 'date') else i

            # Skip first 'period' days (need data for BB calculation)
            if pd.isna(row['UpperBand']):
                self.portfolio_value.append(self.starting_cash)
                continue

            upper_band = row['UpperBand']
            lower_band = row['LowerBand']
            middle_band = row['MA']

            # Buy signal: Price touches or goes below lower band
            if current_price <= lower_band and position == 0:
                self.shares = self.cash // current_price
                self.cash -= self.shares * current_price
                position = 1

                self.trades.append({
                    'date': current_date,
                    'action': 'BUY',
                    'price': current_price,
                    'shares': self.shares,
                    'signal': 'Lower Band Touch',
                    'cash_remaining': self.cash
                })
                print(f"BUY: {self.shares} shares at ${current_price:.2f} (Lower Band) on {current_date}")

            # Sell signal: Price touches or goes above upper band
            elif current_price >= upper_band and position == 1:
                self.cash += self.shares * current_price

                self.trades.append({
                    'date': current_date,
                    'action': 'SELL',
                    'price': current_price,
                    'shares': self.shares,
                    'signal': 'Upper Band Touch',
                    'cash_total': self.cash
                })
                print(f"SELL: {self.shares} shares at ${current_price:.2f} (Upper Band) on {current_date}")
                self.shares = 0
                position = 0

            # Calculate current portfolio value
            current_portfolio_value = self.cash + (self.shares * current_price)
            self.portfolio_value.append(current_portfolio_value)

        return self.calculate_performance()

  def calculate_performance(self):
      """
      Calculate strategy performance metrics
      """
      final_value = self.portfolio_value[-1]
      total_return = (final_value - self.starting_cash) / self.starting_cash

      # Calculate some basic metrics
      portfolio_series = pd.Series(self.portfolio_value)
      daily_returns = portfolio_series.pct_change().dropna()

      # Sharpe ratio (simplified - assuming 0% risk-free rate)
      sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0

      # Maximum drawdown
      rolling_max = portfolio_series.expanding().max()
      drawdown = (portfolio_series - rolling_max) / rolling_max
      max_drawdown = drawdown.min()

      # Win rate
      profitable_trades = 0
      total_trades = len(self.trades) // 2  # Buy and sell pairs

      for i in range(0, len(self.trades), 2):
          if i + 1 < len(self.trades):
              buy_price = self.trades[i]['price']
              sell_price = self.trades[i + 1]['price']
              if sell_price > buy_price:
                  profitable_trades += 1

      win_rate = profitable_trades / total_trades if total_trades > 0 else 0

      results = {
          'starting_cash': self.starting_cash,
          'final_value': final_value,
          'total_return': total_return * 100,  # Convert to percentage
          'total_trades': total_trades,
          'win_rate': win_rate * 100,  # Convert to percentage
          'sharpe_ratio': sharpe_ratio,
          'max_drawdown': max_drawdown * 100,  # Convert to percentage
          'portfolio_values': self.portfolio_value
      }

      return results

def run_backtest_example():
    """
    Example of how to run a Bollinger Bands backtest
    """
    # Create sample data with more realistic BB patterns
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    np.random.seed(42)  # For reproducible results

    # Create price data that oscillates (good for mean reversion)
    prices = [100]  # Starting price
    for i in range(1, len(dates)):
        # Add cyclical pattern + noise for realistic BB testing
        cycle = 5 * np.sin(i * 0.1) + 2 * np.sin(i * 0.05)  # Multi-frequency oscillation
        noise = np.random.normal(0, 1.5)
        trend = 0.002 * i  # Small upward trend

        price_change = (cycle + noise + trend) / 100
        new_price = prices[-1] * (1 + price_change)
        prices.append(max(new_price, 50))  # Floor at $50

    sample_data = pd.DataFrame({
        'Date': dates,
        'Open': [p + np.random.normal(0, 0.5) for p in prices],
        'High': [p + abs(np.random.normal(1, 0.5)) for p in prices],
        'Low': [p - abs(np.random.normal(1, 0.5)) for p in prices],
        'Close': prices,
        'Volume': [int(np.random.uniform(800000, 1200000)) for _ in prices]
    })
    sample_data.set_index('Date', inplace=True)

    # Run the backtest
    backtester = bb_backtest(starting_cash=10000)
    results = backtester.backtest_bollinger_strategy(sample_data)

    # Print results
    print("\n" + "="*50)
    print("BOLLINGER BANDS BACKTEST RESULTS")
    print("="*50)
    print(f"Starting Cash: ${results['starting_cash']:,.2f}")
    print(f"Final Value: ${results['final_value']:,.2f}")
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Total Trades: {results['total_trades']}")
    print(f"Win Rate: {results['win_rate']:.1f}%")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['max_drawdown']:.2f}%")

    print("\n" + "="*50)
    print("TRADE HISTORY")
    print("="*50)
    for trade in backtester.trades:
        action = trade['action']
        shares = trade['shares']
        price = trade['price']
        signal = trade.get('signal', 'N/A')
        print(f"{action}: {shares} shares at ${price:.2f} ({signal})")

    return results

if __name__ == "__main__":
    results = run_backtest_example()
