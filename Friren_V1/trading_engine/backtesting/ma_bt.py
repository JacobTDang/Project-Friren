import pandas as pd
import numpy as np

class MA_backtester:
    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.shares = 0
        self.portfolio_value = []
        self.trades = []

    def backtest_ma_strategy(self, df):
        """
        Backtest strategy to test moving average strategy
        Buy when SMA 10 > SMA 20, sell when SMA 10 < SMA 20
        """
        # Add the moving averages to data
        df = df.copy()
        df['SMA_10'] = df['Close'].rolling(10).mean()
        df['SMA_20'] = df['Close'].rolling(20).mean()

        # Tracking my position
        position = 0

        for i, row in df.iterrows():
            current_price = row['Close']
            current_date = row.name if hasattr(row.name, 'date') else i

            # Skip the first 20 days (need SMA_20 to be calculated)
            if pd.isna(row['SMA_20']):  # Fixed: check SMA_20 instead of SMA_10
                self.portfolio_value.append(self.starting_cash)
                continue

            # Generate signals
            sma_10 = row['SMA_10']
            sma_20 = row['SMA_20']

            # Buy signal if SMA 10 crosses above the SMA 20
            if sma_10 > sma_20 and position == 0:
                self.shares = self.cash // current_price
                self.cash -= self.shares * current_price
                position = 1

                self.trades.append({  # Fixed: trades instead of trade
                    'date': current_date,
                    'action': 'BUY',
                    'price': current_price,
                    'shares': self.shares,
                    'cash_remaining': self.cash
                })

                print(f'BUY {self.shares} shares at ${current_price:.2f} on {current_date}')

            elif sma_10 < sma_20 and position == 1:
                # Sell all shares
                self.cash += self.shares * current_price

                self.trades.append({
                    'date': current_date,
                    'action': 'SELL',
                    'price': current_price,
                    'shares': self.shares,
                    'cash_total': self.cash
                })
                print(f"SELL: {self.shares} shares at ${current_price:.2f} on {current_date}")
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
    Example of how to run a backtest
    """
    # Create sample data
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    np.random.seed(42)  # For reproducible results

    # Simulate stock price data with some trend
    price_changes = np.random.normal(0.001, 0.02, len(dates))  # Small daily gains with volatility
    prices = [100]  # Starting price
    for change in price_changes[1:]:
        prices.append(prices[-1] * (1 + change))

    sample_data = pd.DataFrame({
        'Date': dates,
        'Close': prices
    })
    sample_data.set_index('Date', inplace=True)

    # Run the backtest
    backtester = MA_backtester(starting_cash=10000)
    results = backtester.backtest_ma_strategy(sample_data)

    # Print results
    print("\n" + "="*50)
    print("BACKTEST RESULTS")
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
        print(f"{trade['action']}: {trade['shares']} shares at ${trade['price']:.2f}")

    return results

if __name__ == "__main__":
    results = run_backtest_example()
