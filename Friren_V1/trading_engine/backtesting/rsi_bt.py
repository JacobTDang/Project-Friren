import pandas as pd
import numpy as np
from trading_engine.data.data_utils import StockDataTools

class rsi_backtest:
    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.shares = 0
        self.portfolio_value = []
        self.trades = []
        self.SDT = StockDataTools()

    def rsi_strategy(self, df, rsi_period=14):
        """
        Use the SDT to create the RSI strategy
        """
        df_rsi = self.SDT.add_rsi(df, period=rsi_period)

        # Initialize signals
        df_rsi['signal'] = 0  # 0 = hold, 1 = buy, -1 = sell
        df_rsi['position'] = 0  # Track current position

        # Calculate additional indicators for better signals
        df_rsi['rsi_oversold'] = df_rsi['RSI'] <= 30
        df_rsi['rsi_overbought'] = df_rsi['RSI'] >= 70
        df_rsi['rsi_neutral'] = (df_rsi['RSI'] > 30) & (df_rsi['RSI'] < 70)

        current_position = 0

        for i in range(len(df_rsi)):
            if pd.isna(df_rsi.iloc[i]['RSI']):
                continue

            current_price = df_rsi.iloc[i]['Close']
            current_rsi = df_rsi.iloc[i]['RSI']

            # Buy Signal: RSI below 30 (oversold)
            if current_rsi <= 30 and current_position == 0:
                df_rsi.iloc[i, df_rsi.columns.get_loc('signal')] = 1
                current_position = 1

            # Sell Signal: RSI above 70 (overbought)
            elif current_rsi >= 70 and current_position == 1:
                df_rsi.iloc[i, df_rsi.columns.get_loc('signal')] = -1
                current_position = 0

            # Alternative exit: RSI returns to neutral zone (more conservative)
            elif current_rsi >= 50 and current_position == 1:
                df_rsi.iloc[i, df_rsi.columns.get_loc('signal')] = -1
                current_position = 0

            # Track position
            df_rsi.iloc[i, df_rsi.columns.get_loc('position')] = current_position

        return df_rsi

    def backtest_rsi_strategy(self, df, rsi_period=14, oversold_threshold=30, overbought_threshold=70):
        """
        Backtest RSI mean reversion strategy
        Buy when RSI is oversold, sell when RSI is overbought
        """
        # Add RSI to data
        df = df.copy()
        df_rsi = self.SDT.add_rsi(df, period=rsi_period)

        # Track position
        position = 0  # 0 = no position, 1 = long position

        for i, row in df_rsi.iterrows():
            current_price = row['Close']
            current_date = row.name if hasattr(row.name, 'date') else i

            # Skip first 'rsi_period' days (need data for RSI calculation)
            if pd.isna(row['RSI']):
                self.portfolio_value.append(self.starting_cash)
                continue

            current_rsi = row['RSI']

            # Buy signal: RSI below oversold threshold
            if current_rsi <= oversold_threshold and position == 0:
                self.shares = self.cash // current_price
                self.cash -= self.shares * current_price
                position = 1

                self.trades.append({
                    'date': current_date,
                    'action': 'BUY',
                    'price': current_price,
                    'shares': self.shares,
                    'rsi': current_rsi,
                    'signal': f'RSI Oversold ({current_rsi:.1f})',
                    'cash_remaining': self.cash
                })
                print(f"BUY: {self.shares} shares at ${current_price:.2f} (RSI: {current_rsi:.1f}) on {current_date}")

            # Sell signal: RSI above overbought threshold
            elif current_rsi >= overbought_threshold and position == 1:
                self.cash += self.shares * current_price

                self.trades.append({
                    'date': current_date,
                    'action': 'SELL',
                    'price': current_price,
                    'shares': self.shares,
                    'rsi': current_rsi,
                    'signal': f'RSI Overbought ({current_rsi:.1f})',
                    'cash_total': self.cash
                })
                print(f"SELL: {self.shares} shares at ${current_price:.2f} (RSI: {current_rsi:.1f}) on {current_date}")
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

        # Calculate average RSI levels
        buy_rsi_values = [trade['rsi'] for trade in self.trades if trade['action'] == 'BUY']
        sell_rsi_values = [trade['rsi'] for trade in self.trades if trade['action'] == 'SELL']

        avg_buy_rsi = np.mean(buy_rsi_values) if buy_rsi_values else 0
        avg_sell_rsi = np.mean(sell_rsi_values) if sell_rsi_values else 0

        results = {
            'starting_cash': self.starting_cash,
            'final_value': final_value,
            'total_return': total_return * 100,  # Convert to percentage
            'total_trades': total_trades,
            'win_rate': win_rate * 100,  # Convert to percentage
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,  # Convert to percentage
            'avg_buy_rsi': avg_buy_rsi,
            'avg_sell_rsi': avg_sell_rsi,
            'portfolio_values': self.portfolio_value
        }

        return results

def run_backtest_example():
    """
    Example of how to run an RSI backtest
    """
    # Create sample data with more realistic RSI patterns
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    np.random.seed(42)  # For reproducible results

    # Create price data that has oversold/overbought conditions
    prices = [100]  # Starting price
    for i in range(1, len(dates)):
        # Add cyclical pattern + noise for realistic RSI testing
        cycle = 8 * np.sin(i * 0.08) + 3 * np.sin(i * 0.03)  # Multi-frequency oscillation
        noise = np.random.normal(0, 2.0)
        trend = 0.001 * i  # Small upward trend

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
    backtester = rsi_backtest(starting_cash=10000)
    results = backtester.backtest_rsi_strategy(sample_data, oversold_threshold=30, overbought_threshold=70)

    # Print results
    print("\n" + "="*50)
    print("RSI BACKTEST RESULTS")
    print("="*50)
    print(f"Starting Cash: ${results['starting_cash']:,.2f}")
    print(f"Final Value: ${results['final_value']:,.2f}")
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Total Trades: {results['total_trades']}")
    print(f"Win Rate: {results['win_rate']:.1f}%")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['max_drawdown']:.2f}%")
    print(f"Average Buy RSI: {results['avg_buy_rsi']:.1f}")
    print(f"Average Sell RSI: {results['avg_sell_rsi']:.1f}")

    print("\n" + "="*50)
    print("TRADE HISTORY")
    print("="*50)
    for trade in backtester.trades:
        action = trade['action']
        shares = trade['shares']
        price = trade['price']
        rsi = trade['rsi']
        signal = trade.get('signal', 'N/A')
        print(f"{action}: {shares} shares at ${price:.2f} (RSI: {rsi:.1f}) - {signal}")

    return results

if __name__ == "__main__":
    results = run_backtest_example()
