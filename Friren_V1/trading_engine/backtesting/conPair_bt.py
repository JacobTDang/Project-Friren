import pandas as pd
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.stattools import coint, adfuller
import sys
import os

# Add the project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # Go up 2 levels
sys.path.insert(0, project_root)

from trading_engine.data.data_utils import StockDataTools
from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData

class CointegrationAnalyzer:
    """
    Cointegration analysis for pairs trading
    """
    def __init__(self):
        self.pairs_cache = {}

    def find_cointegrated_pairs(self, price_data, method='engle_granger', p_value_threshold=0.05):
        """
        Find cointegrated pairs from price data
        price_data: DataFrame with prices for different assets as columns
        """
        symbols = price_data.columns
        cointegrated_pairs = []

        for i in range(len(symbols)):
            for j in range(i + 1, len(symbols)):
                symbol1, symbol2 = symbols[i], symbols[j]

                # Get price series
                prices1 = price_data[symbol1].dropna()
                prices2 = price_data[symbol2].dropna()

                # Align data
                aligned_data = pd.concat([prices1, prices2], axis=1).dropna()
                if len(aligned_data) < 50:  # Need sufficient data
                    continue

                p1, p2 = aligned_data.iloc[:, 0], aligned_data.iloc[:, 1]

                # Test cointegration
                try:
                    if method == 'engle_granger':
                        coint_stat, p_value, critical_values = coint(p1, p2)

                        if p_value < p_value_threshold:
                            # Calculate hedge ratio and half-life
                            hedge_ratio = self.calculate_hedge_ratio(p1, p2)
                            spread = p1 - hedge_ratio * p2
                            half_life = self.calculate_half_life(spread)

                            cointegrated_pairs.append({
                                'symbol1': symbol1,
                                'symbol2': symbol2,
                                'p_value': p_value,
                                'hedge_ratio': hedge_ratio,
                                'half_life': half_life,
                                'spread_mean': spread.mean(),
                                'spread_std': spread.std()
                            })

                except Exception as e:
                    continue

        return sorted(cointegrated_pairs, key=lambda x: x['p_value'])

    def calculate_hedge_ratio(self, y, x):
        """Calculate optimal hedge ratio using linear regression"""
        x_reshaped = x.values.reshape(-1, 1)
        reg = LinearRegression().fit(x_reshaped, y.values)
        return reg.coef_[0]

    def calculate_half_life(self, spread):
        """Calculate half-life of mean reversion using AR(1) model"""
        try:
            spread_lag = spread.shift(1).dropna()
            spread_diff = spread.diff().dropna()

            # Align the series
            aligned_data = pd.concat([spread_diff, spread_lag], axis=1).dropna()
            if len(aligned_data) < 10:
                return np.nan

            y = aligned_data.iloc[:, 0].values
            x = aligned_data.iloc[:, 1].values

            # OLS regression: Δy = α + βy_{t-1} + ε
            x_with_const = np.column_stack([np.ones(len(x)), x])
            coeffs = np.linalg.lstsq(x_with_const, y, rcond=None)[0]
            beta = coeffs[1]

            if beta >= 0:  # No mean reversion
                return np.nan

            half_life = -np.log(2) / np.log(1 + beta)
            return half_life

        except Exception:
            return np.nan

class CointegrationPairsBacktest:
    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.positions = {}  # Track positions for multiple pairs
        self.portfolio_value = []
        self.trades = []
        self.SDT = StockDataTools()
        self.analyzer = CointegrationAnalyzer()

    def add_sector_features(self, df, symbol, sector_etf='SPY'):
        """Add sector relative strength features"""
        df_enhanced = df.copy()

        # Get sector ETF data (simplified - using SPY as market proxy)
        try:
            sector_data = self.SDT.extract_data(sector_etf, period="2y", interval="1d")

            # Align dates
            aligned_data = pd.merge(df_enhanced, sector_data[['Close']],
                                  left_index=True, right_index=True,
                                  how='left', suffixes=('', '_sector'))

            # Calculate relative performance
            aligned_data['sector_relative_return'] = (aligned_data['Close'].pct_change() -
                                                    aligned_data['Close_sector'].pct_change())

            # Rolling relative strength
            aligned_data['sector_relative_strength'] = (
                aligned_data['sector_relative_return'].rolling(20).mean()
            )

            return aligned_data

        except Exception as e:
            print(f"Could not fetch sector data: {e}")
            df_enhanced['sector_relative_return'] = 0
            df_enhanced['sector_relative_strength'] = 0
            return df_enhanced

    def calculate_pair_features(self, prices1, prices2, hedge_ratio, lookback=60):
        """Calculate pair trading features"""
        # Ensure we have aligned data
        aligned_data = pd.concat([prices1, prices2], axis=1).dropna()
        if len(aligned_data) < lookback:
            return pd.DataFrame()

        p1, p2 = aligned_data.iloc[:, 0], aligned_data.iloc[:, 1]

        # Calculate spread
        spread = p1 - hedge_ratio * p2

        # Rolling statistics
        rolling_mean = spread.rolling(lookback).mean()
        rolling_std = spread.rolling(lookback).std()

        # Z-score
        zscore = (spread - rolling_mean) / rolling_std

        # Half-life calculation (rolling)
        half_lives = []
        for i in range(lookback, len(spread)):
            window_spread = spread.iloc[i-lookback:i]
            half_life = self.analyzer.calculate_half_life(window_spread)
            half_lives.append(half_life)

        # Create results DataFrame
        results = pd.DataFrame(index=spread.index[lookback:])
        results['pair_spread'] = spread.iloc[lookback:].values
        results['pair_zscore'] = zscore.iloc[lookback:].values
        results['pair_half_life'] = half_lives
        results['spread_mean'] = rolling_mean.iloc[lookback:].values
        results['spread_std'] = rolling_std.iloc[lookback:].values

        return results

    def backtest_pairs_strategy(self, symbol1, symbol2, start_date='2023-01-01',
                               entry_threshold=2.0, exit_threshold=0.5,
                               max_holding_days=30):
        """
        Backtest pairs trading strategy
        """
        print(f"Backtesting pairs strategy: {symbol1} vs {symbol2}")

        # Get price data for both symbols (same period as discovery)
        try:
            data1 = self.SDT.extract_data(symbol1, period="1y", interval="1d")
            data2 = self.SDT.extract_data(symbol2, period="1y", interval="1d")
        except Exception as e:
            print(f"Could not fetch data: {e}")
            return {}

        # Filter by start date
        data1 = data1[data1.index >= start_date]
        data2 = data2[data2.index >= start_date]

        # Test cointegration on training period (first 80% of data)
        train_size = int(0.8 * min(len(data1), len(data2)))
        train_data = pd.concat([data1['Close'][:train_size], data2['Close'][:train_size]],
                              axis=1, keys=[symbol1, symbol2]).dropna()

        if len(train_data) < 50:
            print("Insufficient training data")
            return {}

        # Find optimal hedge ratio
        hedge_ratio = self.analyzer.calculate_hedge_ratio(train_data[symbol1], train_data[symbol2])

        # Test cointegration
        try:
            coint_stat, p_value, critical_values = coint(train_data[symbol1], train_data[symbol2])
            print(f"Cointegration test - p-value: {p_value:.4f}, hedge ratio: {hedge_ratio:.4f}")

            if p_value > 0.05:
                print("Pairs are not significantly cointegrated")
                return {}
        except:
            print("Cointegration test failed")
            return {}

        # Calculate pair features for full period
        pair_features = self.calculate_pair_features(data1['Close'], data2['Close'], hedge_ratio)

        if pair_features.empty:
            print("Could not calculate pair features")
            return {}

        # Add sector features
        data1_enhanced = self.add_sector_features(data1, symbol1)

        # Initialize tracking variables
        position = 0  # 0 = no position, 1 = long spread, -1 = short spread
        entry_date = None
        entry_spread = 0
        shares1, shares2 = 0, 0

        # Backtest loop
        for date, row in pair_features.iterrows():
            try:
                current_price1 = data1.loc[date, 'Close']
                current_price2 = data2.loc[date, 'Close']
            except KeyError:
                continue

            zscore = row['pair_zscore']
            spread = row['pair_spread']
            half_life = row['pair_half_life']

            # Skip if half-life is too long or undefined
            if pd.isna(half_life) or half_life > max_holding_days:
                continue

            # Calculate current portfolio value
            portfolio_value = self.cash
            if position != 0:
                portfolio_value += shares1 * current_price1 + shares2 * current_price2
            self.portfolio_value.append(portfolio_value)

            # Entry signals
            if position == 0:
                # Long spread (buy symbol1, sell symbol2) when spread is low
                if zscore <= -entry_threshold:
                    # Calculate position sizes
                    total_allocation = self.cash * 0.5  # Use 50% of cash per trade
                    shares1 = int(total_allocation / (current_price1 + hedge_ratio * current_price2))
                    shares2 = -int(shares1 * hedge_ratio)  # Short position

                    # Execute trades
                    cost1 = shares1 * current_price1
                    cost2 = abs(shares2) * current_price2  # Cost to short
                    total_cost = cost1 + cost2

                    if total_cost <= self.cash:
                        self.cash -= total_cost
                        position = 1
                        entry_date = date
                        entry_spread = spread

                        self.trades.append({
                            'date': date,
                            'action': 'LONG_SPREAD',
                            'symbol1': symbol1,
                            'symbol2': symbol2,
                            'price1': current_price1,
                            'price2': current_price2,
                            'shares1': shares1,
                            'shares2': shares2,
                            'zscore': zscore,
                            'spread': spread,
                            'half_life': half_life,
                            'hedge_ratio': hedge_ratio,
                            'signal': f'Long spread (Z={zscore:.2f})'
                        })

                        print(f"LONG SPREAD: {shares1} {symbol1} at ${current_price1:.2f}, "
                              f"{shares2} {symbol2} at ${current_price2:.2f} (Z: {zscore:.2f})")

                # Short spread (sell symbol1, buy symbol2) when spread is high
                elif zscore >= entry_threshold:
                    # Calculate position sizes
                    total_allocation = self.cash * 0.5
                    shares1 = -int(total_allocation / (current_price1 + hedge_ratio * current_price2))
                    shares2 = int(abs(shares1) * hedge_ratio)  # Long position

                    # Execute trades
                    cost1 = abs(shares1) * current_price1  # Cost to short
                    cost2 = shares2 * current_price2
                    total_cost = cost1 + cost2

                    if total_cost <= self.cash:
                        self.cash -= total_cost
                        position = -1
                        entry_date = date
                        entry_spread = spread

                        self.trades.append({
                            'date': date,
                            'action': 'SHORT_SPREAD',
                            'symbol1': symbol1,
                            'symbol2': symbol2,
                            'price1': current_price1,
                            'price2': current_price2,
                            'shares1': shares1,
                            'shares2': shares2,
                            'zscore': zscore,
                            'spread': spread,
                            'half_life': half_life,
                            'hedge_ratio': hedge_ratio,
                            'signal': f'Short spread (Z={zscore:.2f})'
                        })

                        print(f"SHORT SPREAD: {shares1} {symbol1} at ${current_price1:.2f}, "
                              f"{shares2} {symbol2} at ${current_price2:.2f} (Z: {zscore:.2f})")

            # Exit signals
            elif position != 0:
                days_held = (date - entry_date).days if entry_date else 0

                # Exit conditions
                exit_signal = False
                exit_reason = ""

                # Mean reversion exit
                if position == 1 and zscore >= -exit_threshold:
                    exit_signal = True
                    exit_reason = f"Long spread mean revert (Z={zscore:.2f})"
                elif position == -1 and zscore <= exit_threshold:
                    exit_signal = True
                    exit_reason = f"Short spread mean revert (Z={zscore:.2f})"

                # Maximum holding period exit
                elif days_held >= max_holding_days:
                    exit_signal = True
                    exit_reason = f"Max holding period ({days_held} days)"

                # Stop loss: spread moving against us significantly
                elif position == 1 and zscore <= -entry_threshold * 1.5:
                    exit_signal = True
                    exit_reason = f"Stop loss - spread diverging (Z={zscore:.2f})"
                elif position == -1 and zscore >= entry_threshold * 1.5:
                    exit_signal = True
                    exit_reason = f"Stop loss - spread diverging (Z={zscore:.2f})"

                if exit_signal:
                    # Close position
                    proceeds1 = shares1 * current_price1 if shares1 > 0 else -shares1 * current_price1
                    proceeds2 = shares2 * current_price2 if shares2 > 0 else -shares2 * current_price2
                    total_proceeds = proceeds1 + proceeds2

                    self.cash += total_proceeds

                    # Calculate profit
                    entry_value = abs(shares1) * self.trades[-1]['price1'] + abs(shares2) * self.trades[-1]['price2']
                    profit = total_proceeds - entry_value if position == 1 else entry_value - total_proceeds

                    self.trades.append({
                        'date': date,
                        'action': 'CLOSE_SPREAD',
                        'symbol1': symbol1,
                        'symbol2': symbol2,
                        'price1': current_price1,
                        'price2': current_price2,
                        'shares1': shares1,
                        'shares2': shares2,
                        'zscore': zscore,
                        'spread': spread,
                        'profit': profit,
                        'days_held': days_held,
                        'signal': exit_reason
                    })

                    print(f"CLOSE SPREAD: Profit ${profit:.2f}, Days held: {days_held}, "
                          f"Exit Z: {zscore:.2f} - {exit_reason}")

                    # Reset position
                    position = 0
                    shares1, shares2 = 0, 0
                    entry_date = None

        return self.calculate_performance()

    def calculate_performance(self):
        """Calculate strategy performance metrics"""
        if not self.portfolio_value:
            return {}

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

        # Trade analysis
        entry_trades = [t for t in self.trades if t['action'] in ['LONG_SPREAD', 'SHORT_SPREAD']]
        exit_trades = [t for t in self.trades if t['action'] == 'CLOSE_SPREAD']

        total_trades = len(exit_trades)
        profitable_trades = len([t for t in exit_trades if t['profit'] > 0])
        win_rate = profitable_trades / total_trades if total_trades > 0 else 0

        # Pairs-specific metrics
        avg_holding_days = np.mean([t['days_held'] for t in exit_trades]) if exit_trades else 0
        avg_entry_zscore = np.mean([abs(t['zscore']) for t in entry_trades]) if entry_trades else 0
        avg_exit_zscore = np.mean([abs(t['zscore']) for t in exit_trades]) if exit_trades else 0
        total_profit = sum([t['profit'] for t in exit_trades])

        results = {
            'starting_cash': self.starting_cash,
            'final_value': final_value,
            'total_return': total_return * 100,
            'total_trades': total_trades,
            'win_rate': win_rate * 100,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,
            'total_profit': total_profit,
            'avg_holding_days': avg_holding_days,
            'avg_entry_zscore': avg_entry_zscore,
            'avg_exit_zscore': avg_exit_zscore,
            'portfolio_values': self.portfolio_value
        }

        return results

def run_pairs_backtest_example():
    """Example of how to run a cointegration pairs backtest"""
    print("Running Cointegration Pairs Trading Backtest...")

    # Example with tech stocks that might be cointegrated
    symbol1 = "AAPL"
    symbol2 = "MSFT"

    # Run the backtest
    backtester = CointegrationPairsBacktest(starting_cash=50000)
    results = backtester.backtest_pairs_strategy(
        symbol1=symbol1,
        symbol2=symbol2,
        start_date='2023-01-01',
        entry_threshold=2.0,
        exit_threshold=0.5,
        max_holding_days=30
    )

    if not results:
        print("Backtest failed - pairs may not be cointegrated")
        return {}

    # Print results
    print("\n" + "="*60)
    print("COINTEGRATION PAIRS TRADING BACKTEST RESULTS")
    print("="*60)
    print(f"Pair: {symbol1} vs {symbol2}")
    print(f"Starting Cash: ${results['starting_cash']:,.2f}")
    print(f"Final Value: ${results['final_value']:,.2f}")
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Total Profit: ${results['total_profit']:,.2f}")
    print(f"Total Trades: {results['total_trades']}")
    print(f"Win Rate: {results['win_rate']:.1f}%")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['max_drawdown']:.2f}%")
    print(f"Average Holding Days: {results['avg_holding_days']:.1f}")
    print(f"Average Entry |Z-Score|: {results['avg_entry_zscore']:.2f}")
    print(f"Average Exit |Z-Score|: {results['avg_exit_zscore']:.2f}")

    print("\n" + "="*60)
    print("SAMPLE TRADES")
    print("="*60)
    for i, trade in enumerate(backtester.trades[:10]):  # Show first 10 trades
        action = trade['action']
        signal = trade.get('signal', 'N/A')
        zscore = trade.get('zscore', 0)
        profit = trade.get('profit', 0)

        if action == 'CLOSE_SPREAD':
            print(f"CLOSE: Profit ${profit:.2f}, Z: {zscore:.2f} - {signal}")
        else:
            print(f"{action}: Z: {zscore:.2f} - {signal}")

    if len(backtester.trades) > 10:
        print(f"... and {len(backtester.trades) - 10} more trades")

    return results

def find_best_pairs_example():
    """Example of finding the best cointegrated pairs"""
    print("Finding best cointegrated pairs...")

    # List of stocks to test
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'BAC', 'GS']

    # Get data for all symbols
    sdt = StockDataTools()
    price_data = pd.DataFrame()

    for symbol in symbols:
        try:
            data = sdt.extract_data(symbol, period="1y", interval="1d")
            price_data[symbol] = data['Close']
            print(f"Fetched data for {symbol}")
        except Exception as e:
            print(f"Could not fetch data for {symbol}: {e}")
            continue

    if price_data.empty:
        print("No price data available")
        return []

    # Find cointegrated pairs
    analyzer = CointegrationAnalyzer()
    pairs = analyzer.find_cointegrated_pairs(price_data, p_value_threshold=0.05)

    print(f"\nFound {len(pairs)} cointegrated pairs:")
    print("="*60)
    for i, pair in enumerate(pairs[:5]):  # Show top 5 pairs
        print(f"{i+1}. {pair['symbol1']} vs {pair['symbol2']}")
        print(f"   P-value: {pair['p_value']:.4f}")
        print(f"   Hedge ratio: {pair['hedge_ratio']:.4f}")
        print(f"   Half-life: {pair['half_life']:.1f} days")
        print()

    return pairs

if __name__ == "__main__":
    # First find good pairs
    pairs = find_best_pairs_example()

    # Then run backtest on best pair
    if pairs:
        best_pair = pairs[0]
        print(f"\nRunning backtest on best pair: {best_pair['symbol1']} vs {best_pair['symbol2']}")

        backtester = CointegrationPairsBacktest(starting_cash=50000)
        results = backtester.backtest_pairs_strategy(
            symbol1=best_pair['symbol1'],
            symbol2=best_pair['symbol2'],
            start_date='2023-01-01'
        )
    else:
        # Fallback to example
        results = run_pairs_backtest_example()
