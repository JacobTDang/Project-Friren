import pandas as pd
import numpy as np
import sys
import os

# Fix import path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from trading_engine.data.data_utils import StockDataTools

class SimpleJumpDiffusionBacktest:
    """
    Clean, simple Jump Diffusion strategy focused on detecting price jumps
    """
    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.shares = 0
        self.portfolio_value = []
        self.trades = []
        self.SDT = StockDataTools()

    def detect_jumps(self, df, jump_threshold=0.03, window=20):
        """
        Simple jump detection: Look for daily returns > threshold
        """
        df_jump = df.copy()

        # Calculate daily returns
        df_jump['daily_return'] = df_jump['Close'].pct_change()

        # Calculate rolling volatility
        df_jump['rolling_vol'] = df_jump['daily_return'].rolling(window).std()

        # Detect jumps: returns more than N standard deviations from mean
        df_jump['jump_signal'] = 0
        df_jump['jump_magnitude'] = 0
        df_jump['volatility_signal'] = 0

        for i in range(window, len(df_jump)):
            current_return = df_jump['daily_return'].iloc[i]
            rolling_vol = df_jump['rolling_vol'].iloc[i]

            # Simple jump detection
            if abs(current_return) > jump_threshold:
                df_jump['jump_signal'].iloc[i] = 1
                df_jump['jump_magnitude'].iloc[i] = current_return

            # Volatility-based detection
            if rolling_vol > 0:
                z_score = abs(current_return) / rolling_vol
                if z_score > 2.0:  # 2 standard deviations
                    df_jump['volatility_signal'].iloc[i] = 1

        return df_jump

    def add_sentiment_proxy(self, df):
        """
        Simple sentiment proxy based on price momentum
        """
        df_sent = df.copy()

        # Use moving averages as momentum proxy
        df_sent['sma_5'] = df_sent['Close'].rolling(5).mean()
        df_sent['sma_20'] = df_sent['Close'].rolling(20).mean()

        # Sentiment signal: price above/below moving averages
        df_sent['momentum_signal'] = 0
        df_sent['trend_strength'] = 0

        for i in range(20, len(df_sent)):
            current_price = df_sent['Close'].iloc[i]
            sma_5 = df_sent['sma_5'].iloc[i]
            sma_20 = df_sent['sma_20'].iloc[i]

            # Positive momentum (bullish sentiment proxy)
            if current_price > sma_5 > sma_20:
                df_sent['momentum_signal'].iloc[i] = 1
                df_sent['trend_strength'].iloc[i] = (current_price - sma_20) / sma_20

            # Negative momentum (bearish sentiment proxy)
            elif current_price < sma_5 < sma_20:
                df_sent['momentum_signal'].iloc[i] = -1
                df_sent['trend_strength'].iloc[i] = (current_price - sma_20) / sma_20

        return df_sent

    def backtest_simple_jump_strategy(self, df, min_jump_size=0.025, holding_days=3):
        """
        Simple jump diffusion strategy:
        1. Detect price jumps
        2. Enter position on positive jumps with good momentum
        3. Exit after fixed holding period or stop loss
        """
        print(f"Running Simple Jump Diffusion Strategy...")

        # Add jump detection
        df_enhanced = self.detect_jumps(df, jump_threshold=min_jump_size)

        # Add sentiment proxy
        df_enhanced = self.add_sentiment_proxy(df_enhanced)

        # Trading variables
        position = 0  # 0 = no position, 1 = long
        entry_date = None
        entry_price = 0
        days_held = 0

        for i in range(25, len(df_enhanced)):  # Start after warm-up period
            current_price = df_enhanced['Close'].iloc[i]
            current_date = df_enhanced.index[i]

            # Get signals
            jump_signal = df_enhanced['jump_signal'].iloc[i]
            jump_magnitude = df_enhanced['jump_magnitude'].iloc[i]
            momentum_signal = df_enhanced['momentum_signal'].iloc[i]
            trend_strength = df_enhanced['trend_strength'].iloc[i]
            volatility_signal = df_enhanced['volatility_signal'].iloc[i]

            # Debug output every 50 days
            if i % 50 == 0:
                print(f"Day {i}: Price ${current_price:.2f}, Jump: {jump_signal}, "
                      f"Momentum: {momentum_signal}, Vol Signal: {volatility_signal}")

            # Update days held
            if position == 1:
                days_held = (current_date - entry_date).days if entry_date else days_held + 1

            # ENTRY LOGIC: Look for positive jumps with good momentum
            if position == 0:
                # Enter on positive jump with positive momentum
                if ((jump_signal == 1 and jump_magnitude > 0 and momentum_signal >= 0) or
                    (volatility_signal == 1 and momentum_signal == 1 and trend_strength > 0.01)):

                    # Position sizing: use 40% of available cash
                    position_size = self.cash * 0.4
                    shares_to_buy = int(position_size / current_price)

                    if shares_to_buy > 0:
                        cost = shares_to_buy * current_price
                        if cost <= self.cash:
                            self.shares = shares_to_buy
                            self.cash -= cost
                            position = 1
                            entry_date = current_date
                            entry_price = current_price
                            days_held = 0

                            self.trades.append({
                                'date': current_date,
                                'action': 'BUY',
                                'price': current_price,
                                'shares': self.shares,
                                'jump_magnitude': jump_magnitude,
                                'momentum_signal': momentum_signal,
                                'trend_strength': trend_strength,
                                'signal': f'Jump+Momentum (Return: {jump_magnitude*100:.1f}%)',
                                'cash_remaining': self.cash
                            })

                            print(f"BUY: {self.shares} shares at ${current_price:.2f} "
                                  f"(Jump: {jump_magnitude*100:.1f}%, Momentum: {momentum_signal})")

            # EXIT LOGIC
            elif position == 1:
                exit_signal = False
                exit_reason = ""

                # Exit after holding period
                if days_held >= holding_days:
                    exit_signal = True
                    exit_reason = f"Holding period complete ({days_held} days)"

                # Stop loss: 6% loss
                elif current_price < entry_price * 0.94:
                    exit_signal = True
                    exit_reason = f"Stop loss ({((current_price/entry_price - 1) * 100):.1f}%)"

                # Take profit: 8% gain
                elif current_price > entry_price * 1.08:
                    exit_signal = True
                    exit_reason = f"Take profit ({((current_price/entry_price - 1) * 100):.1f}%)"

                # Momentum reversal exit
                elif momentum_signal == -1 and days_held >= 2:
                    exit_signal = True
                    exit_reason = "Momentum reversal"

                if exit_signal:
                    # Close position
                    proceeds = self.shares * current_price
                    self.cash += proceeds
                    profit = proceeds - (self.shares * entry_price)

                    self.trades.append({
                        'date': current_date,
                        'action': 'SELL',
                        'price': current_price,
                        'shares': self.shares,
                        'profit': profit,
                        'return_pct': (current_price / entry_price - 1) * 100,
                        'days_held': days_held,
                        'signal': exit_reason
                    })

                    print(f"SELL: {self.shares} shares at ${current_price:.2f} "
                          f"(Profit: ${profit:.2f}, Return: {((current_price/entry_price - 1) * 100):.1f}%) - {exit_reason}")

                    # Reset position
                    self.shares = 0
                    position = 0
                    entry_date = None
                    days_held = 0

            # Update portfolio value
            portfolio_value = self.cash + (self.shares * current_price)
            self.portfolio_value.append(portfolio_value)

        return self.calculate_performance()

    def calculate_performance(self):
        """Calculate strategy performance"""
        if not self.portfolio_value:
            return {'error': 'No portfolio data'}

        final_value = self.portfolio_value[-1]
        total_return = (final_value - self.starting_cash) / self.starting_cash

        # Basic metrics
        portfolio_series = pd.Series(self.portfolio_value)
        daily_returns = portfolio_series.pct_change().dropna()

        sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0

        # Drawdown
        rolling_max = portfolio_series.expanding().max()
        drawdown = (portfolio_series - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        # Trade analysis
        buy_trades = [t for t in self.trades if t['action'] == 'BUY']
        sell_trades = [t for t in self.trades if t['action'] == 'SELL']

        total_trades = len(sell_trades)
        profitable_trades = len([t for t in sell_trades if t['profit'] > 0])
        win_rate = profitable_trades / total_trades if total_trades > 0 else 0

        total_profit = sum([t['profit'] for t in sell_trades])
        avg_holding_days = np.mean([t['days_held'] for t in sell_trades]) if sell_trades else 0

        # Jump-specific metrics
        avg_jump_magnitude = np.mean([abs(t.get('jump_magnitude', 0)) for t in buy_trades]) if buy_trades else 0

        return {
            'starting_cash': self.starting_cash,
            'final_value': final_value,
            'total_return': total_return * 100,
            'total_trades': total_trades,
            'win_rate': win_rate * 100,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,
            'total_profit': total_profit,
            'avg_holding_days': avg_holding_days,
            'avg_jump_magnitude': avg_jump_magnitude * 100,
            'portfolio_values': self.portfolio_value
        }

def create_test_data_with_jumps():
    """Create realistic test data with obvious jumps"""
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    np.random.seed(42)

    prices = [100]

    for i in range(1, len(dates)):
        # Normal price movement (85% of time)
        if np.random.random() < 0.85:
            daily_change = np.random.normal(0.001, 0.015)  # Small daily moves

        # Small jumps (10% of time)
        elif np.random.random() < 0.95:
            daily_change = np.random.choice([-0.03, 0.03])  # 3% jumps

        # Large jumps (5% of time)
        else:
            daily_change = np.random.choice([-0.06, 0.06])  # 6% jumps

        new_price = prices[-1] * (1 + daily_change)
        prices.append(max(new_price, 50))  # Floor at $50

    return pd.DataFrame({
        'Date': dates,
        'Open': [p * 0.999 for p in prices],
        'High': [p * 1.005 for p in prices],
        'Low': [p * 0.995 for p in prices],
        'Close': prices,
        'Volume': [int(np.random.uniform(800000, 1200000)) for _ in prices]
    }).set_index('Date')

def run_simple_jump_backtest():
    """Run the simple jump diffusion backtest"""
    print("="*60)
    print("SIMPLE JUMP DIFFUSION BACKTEST")
    print("="*60)

    # Create test data with clear jumps
    test_data = create_test_data_with_jumps()

    # Run backtest
    backtester = SimpleJumpDiffusionBacktest(starting_cash=25000)
    results = backtester.backtest_simple_jump_strategy(
        test_data,
        min_jump_size=0.025,  # 2.5% minimum jump
        holding_days=4        # Hold for 4 days
    )

    # Print results
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"Starting Cash: ${results['starting_cash']:,.2f}")
    print(f"Final Value: ${results['final_value']:,.2f}")
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Total Profit: ${results['total_profit']:,.2f}")
    print(f"Total Trades: {results['total_trades']}")
    print(f"Win Rate: {results['win_rate']:.1f}%")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['max_drawdown']:.2f}%")
    print(f"Avg Holding Days: {results['avg_holding_days']:.1f}")
    print(f"Avg Jump Magnitude: {results['avg_jump_magnitude']:.2f}%")

    # Show sample trades
    print(f"\n" + "="*60)
    print("SAMPLE TRADES")
    print("="*60)

    for i, trade in enumerate(backtester.trades[:10]):
        action = trade['action']
        price = trade['price']
        signal = trade.get('signal', 'N/A')

        if action == 'BUY':
            shares = trade['shares']
            jump_mag = trade.get('jump_magnitude', 0) * 100
            momentum = trade.get('momentum_signal', 0)
            print(f"BUY: {shares} shares @ ${price:.2f} (Jump: {jump_mag:.1f}%, Momentum: {momentum}) - {signal}")
        else:
            profit = trade.get('profit', 0)
            return_pct = trade.get('return_pct', 0)
            days = trade.get('days_held', 0)
            print(f"SELL: @ ${price:.2f} (Profit: ${profit:.2f}, Return: {return_pct:.1f}%, {days} days) - {signal}")

    if len(backtester.trades) > 10:
        print(f"... and {len(backtester.trades) - 10} more trades")

    return results

if __name__ == "__main__":
    results = run_simple_jump_backtest()
