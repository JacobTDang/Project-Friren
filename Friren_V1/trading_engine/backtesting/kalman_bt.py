import pandas as pd
import numpy as np
from scipy.optimize import minimize
import sys
import os

# Fix import path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from trading_engine.data.data_utils import StockDataTools

class KalmanFilter:
    """
    Kalman Filter for dynamic price estimation and mean reversion
    """
    def __init__(self, observation_covariance=1.0, transition_covariance=1.0):
        self.observation_covariance = observation_covariance
        self.transition_covariance = transition_covariance
        self.state_mean = 0.0
        self.state_covariance = 1.0
        self.history = []

    def update(self, observation):
        """Update filter with new price observation"""
        # Prediction step
        predicted_state_mean = self.state_mean
        predicted_state_covariance = self.state_covariance + self.transition_covariance

        # Update step
        kalman_gain = predicted_state_covariance / (predicted_state_covariance + self.observation_covariance)

        self.state_mean = predicted_state_mean + kalman_gain * (observation - predicted_state_mean)
        self.state_covariance = (1 - kalman_gain) * predicted_state_covariance

        # Store results
        result = {
            'fair_value': self.state_mean,
            'deviation': observation - self.state_mean,
            'confidence': 1.0 / self.state_covariance,
            'kalman_gain': kalman_gain
        }

        self.history.append(result)
        return result

class KalmanMeanReversionBacktest:
    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.shares = 0
        self.portfolio_value = []
        self.trades = []
        self.SDT = StockDataTools()

    def add_kalman_features(self, df, observation_cov=1.0, transition_cov=1.0):
        """Add Kalman filter features to dataframe"""
        df_kalman = df.copy()

        # Initialize Kalman filter
        kf = KalmanFilter(observation_cov, transition_cov)

        # Initialize columns
        df_kalman['kalman_fair_value'] = np.nan
        df_kalman['kalman_deviation'] = np.nan
        df_kalman['kalman_velocity'] = np.nan
        df_kalman['kalman_confidence'] = np.nan
        df_kalman['kalman_zscore'] = np.nan

        fair_values = []
        deviations = []

        for idx, row in df_kalman.iterrows():
            price = row['Close']
            result = kf.update(price)

            df_kalman.loc[idx, 'kalman_fair_value'] = result['fair_value']
            df_kalman.loc[idx, 'kalman_deviation'] = result['deviation']
            df_kalman.loc[idx, 'kalman_confidence'] = result['confidence']

            fair_values.append(result['fair_value'])
            deviations.append(result['deviation'])

            # Calculate velocity (rate of change in fair value)
            if len(fair_values) > 1:
                velocity = fair_values[-1] - fair_values[-2]
                df_kalman.loc[idx, 'kalman_velocity'] = velocity

            # Calculate rolling Z-score of deviations
            if len(deviations) >= 20:
                recent_deviations = deviations[-20:]
                std_dev = np.std(recent_deviations)
                if std_dev > 0:
                    zscore = result['deviation'] / std_dev
                    df_kalman.loc[idx, 'kalman_zscore'] = zscore

        return df_kalman

    def backtest_kalman_strategy(self, df, entry_zscore=1.5, exit_zscore=0.3, confidence_threshold=0.05):
        """
        Backtest Kalman Filter mean reversion strategy
        """
        print("Running Kalman Filter backtest...")

        # Add Kalman features
        df_enhanced = self.add_kalman_features(df)

        # Track position and holding
        position = 0  # 0 = no position, 1 = long position
        entry_price = 0
        day_counter = 0

        for idx, row in df_enhanced.iterrows():
            current_price = row['Close']
            current_date = idx
            day_counter += 1

            # Skip if we don't have enough data
            if pd.isna(row['kalman_zscore']) or pd.isna(row['kalman_confidence']):
                self.portfolio_value.append(self.cash + (self.shares * current_price))
                continue

            fair_value = row['kalman_fair_value']
            deviation = row['kalman_deviation']
            zscore = row['kalman_zscore']
            confidence = row['kalman_confidence']
            velocity = row['kalman_velocity'] if not pd.isna(row['kalman_velocity']) else 0

            # DEBUG: Print some values to see what's happening
            if day_counter % 50 == 0:  # Print every 50 days
                print(f"Day {day_counter}: Price: ${current_price:.2f}, Fair: ${fair_value:.2f}, Z: {zscore:.2f}, Conf: {confidence:.3f}")

            # Only trade if we have sufficient confidence in the estimate
            if confidence < confidence_threshold:
                self.portfolio_value.append(self.cash + (self.shares * current_price))
                continue

            # Long entry: Price significantly below fair value (oversold)
            if zscore <= -entry_zscore and position == 0:
                self.shares = int(self.cash // current_price)
                if self.shares > 0:
                    self.cash -= self.shares * current_price
                    position = 1
                    entry_price = current_price

                    self.trades.append({
                        'date': current_date,
                        'action': 'BUY',
                        'price': current_price,
                        'shares': self.shares,
                        'fair_value': fair_value,
                        'deviation': deviation,
                        'zscore': zscore,
                        'confidence': confidence,
                        'signal': f'Kalman Oversold (Z={zscore:.2f})',
                        'cash_remaining': self.cash
                    })
                    print(f"BUY: {self.shares} shares at ${current_price:.2f} (Fair: ${fair_value:.2f}, Z: {zscore:.2f})")

            # Long exit: Price reverted towards fair value
            elif position == 1 and zscore >= -exit_zscore:
                self.cash += self.shares * current_price
                profit = (current_price - entry_price) * self.shares

                self.trades.append({
                    'date': current_date,
                    'action': 'SELL',
                    'price': current_price,
                    'shares': self.shares,
                    'fair_value': fair_value,
                    'deviation': deviation,
                    'zscore': zscore,
                    'confidence': confidence,
                    'profit': profit,
                    'signal': f'Kalman Mean Revert (Z={zscore:.2f})',
                    'cash_total': self.cash
                })
                print(f"SELL: {self.shares} shares at ${current_price:.2f} (Fair: ${fair_value:.2f}, Z: {zscore:.2f}, Profit: ${profit:.2f})")
                self.shares = 0
                position = 0

            # Calculate current portfolio value
            current_portfolio_value = self.cash + (self.shares * current_price)
            self.portfolio_value.append(current_portfolio_value)

        return self.calculate_performance()

    def calculate_performance(self):
        """Calculate strategy performance metrics"""
        if not self.portfolio_value:
            return {'error': 'No portfolio values calculated'}

        final_value = self.portfolio_value[-1]
        total_return = (final_value - self.starting_cash) / self.starting_cash

        # Calculate basic metrics
        portfolio_series = pd.Series(self.portfolio_value)
        daily_returns = portfolio_series.pct_change().dropna()

        # Sharpe ratio
        sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0

        # Maximum drawdown
        rolling_max = portfolio_series.expanding().max()
        drawdown = (portfolio_series - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        # Win rate and profit analysis
        buy_trades = [t for t in self.trades if t['action'] == 'BUY']
        sell_trades = [t for t in self.trades if t['action'] == 'SELL']

        total_trades = len(sell_trades)
        profitable_trades = len([t for t in sell_trades if t.get('profit', 0) > 0])
        win_rate = profitable_trades / total_trades if total_trades > 0 else 0

        # Kalman-specific metrics
        avg_entry_zscore = np.mean([abs(t['zscore']) for t in buy_trades]) if buy_trades else 0
        avg_exit_zscore = np.mean([abs(t['zscore']) for t in sell_trades]) if sell_trades else 0
        avg_confidence = np.mean([t['confidence'] for t in buy_trades]) if buy_trades else 0
        total_profit = sum([t.get('profit', 0) for t in sell_trades])

        results = {
            'starting_cash': self.starting_cash,
            'final_value': final_value,
            'total_return': total_return * 100,
            'total_trades': total_trades,
            'win_rate': win_rate * 100,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,
            'total_profit': total_profit,
            'avg_entry_zscore': avg_entry_zscore,
            'avg_exit_zscore': avg_exit_zscore,
            'avg_confidence': avg_confidence,
            'portfolio_values': self.portfolio_value
        }

        return results

def run_kalman_backtest_example():
    """Example of how to run a Kalman Filter backtest"""
    # Create sample data with mean-reverting characteristics
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    np.random.seed(42)

    # Generate mean-reverting price series
    prices = [100]
    true_value = 100

    for i in range(1, len(dates)):
        # Add some big deviations occasionally for clear signals
        if i % 30 == 0:  # Every 30 days, create a deviation
            shock = np.random.choice([-8, 8])  # Big price shock
            prices.append(prices[-1] + shock)
        else:
            # Mean reversion to slowly changing true value
            true_value += np.random.normal(0, 0.3)  # Slow drift in fair value

            # Price mean reverts to true value with noise
            reversion_speed = 0.15
            price_change = reversion_speed * (true_value - prices[-1]) + np.random.normal(0, 1.5)
            new_price = prices[-1] + price_change
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
    backtester = KalmanMeanReversionBacktest(starting_cash=10000)
    results = backtester.backtest_kalman_strategy(
        sample_data,
        entry_zscore=1.2,  # Even more lenient
        exit_zscore=0.2,   # More lenient exit
        confidence_threshold=0.01  # Very lenient confidence
    )

    # Print results
    print("\n" + "="*60)
    print("KALMAN FILTER MEAN REVERSION BACKTEST RESULTS")
    print("="*60)
    print(f"Starting Cash: ${results['starting_cash']:,.2f}")
    print(f"Final Value: ${results['final_value']:,.2f}")
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Total Profit: ${results['total_profit']:,.2f}")
    print(f"Total Trades: {results['total_trades']}")
    print(f"Win Rate: {results['win_rate']:.1f}%")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['max_drawdown']:.2f}%")
    print(f"Average Entry Z-Score: {results['avg_entry_zscore']:.2f}")
    print(f"Average Exit Z-Score: {results['avg_exit_zscore']:.2f}")
    print(f"Average Confidence: {results['avg_confidence']:.3f}")

    print("\n" + "="*60)
    print("SAMPLE TRADES")
    print("="*60)
    for i, trade in enumerate(backtester.trades[:10]):  # Show first 10 trades
        action = trade['action']
        shares = trade['shares']
        price = trade['price']
        fair_value = trade.get('fair_value', 0)
        zscore = trade.get('zscore', 0)
        signal = trade.get('signal', 'N/A')

        if action == 'SELL':
            profit = trade.get('profit', 0)
            print(f"{action}: {shares} shares at ${price:.2f} (Fair: ${fair_value:.2f}, Z: {zscore:.2f}, Profit: ${profit:.2f})")
        else:
            print(f"{action}: {shares} shares at ${price:.2f} (Fair: ${fair_value:.2f}, Z: {zscore:.2f})")

        if i >= 9:  # Show first 10 trades
            break

    if len(backtester.trades) > 10:
        print(f"... and {len(backtester.trades) - 10} more trades")

    return results

if __name__ == "__main__":
    results = run_kalman_backtest_example()
