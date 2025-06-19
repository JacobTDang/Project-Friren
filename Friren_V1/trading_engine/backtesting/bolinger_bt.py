import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

class EnhancedBollingerBacktest:
    """
    Enhanced Bollinger Bands backtest with multiple improvements
    """

    def __init__(self, starting_cash=25000):
        self.starting_cash = starting_cash
        self.reset()

    def reset(self):
        """Reset backtest state"""
        self.cash = self.starting_cash
        self.shares = 0
        self.portfolio_value = []
        self.trades = []
        self.daily_returns = []

    def analyze_current_strategy_problems(self, df):
        """
        Analyze why the current Bollinger Bands strategy is failing
        """
        print(" ANALYZING STRATEGY PROBLEMS:")
        print("="*60)

        # Add Bollinger Bands
        df = df.copy()
        df['MA'] = df['Close'].rolling(20).mean()
        df['STD'] = df['Close'].rolling(20).std()
        df['UpperBand'] = df['MA'] + (2 * df['STD'])
        df['LowerBand'] = df['MA'] - (2 * df['STD'])
        df['BB_Position'] = (df['Close'] - df['LowerBand']) / (df['UpperBand'] - df['LowerBand'])
        df['BB_Width'] = (df['UpperBand'] - df['LowerBand']) / df['MA']

        # Analyze band touches
        lower_touches = df[df['Close'] <= df['LowerBand']]
        upper_touches = df[df['Close'] >= df['UpperBand']]

        print(f" BOLLINGER BAND ANALYSIS:")
        print(f"   • Lower band touches: {len(lower_touches)}")
        print(f"   • Upper band touches: {len(upper_touches)}")
        print(f"   • Average BB width: {df['BB_Width'].mean():.3f}")
        print(f"   • BB width std: {df['BB_Width'].std():.3f}")

        # Check market regime
        returns = df['Close'].pct_change()
        volatility = returns.rolling(20).std() * np.sqrt(252)
        trend = (df['Close'].iloc[-1] / df['Close'].iloc[0]) ** (252/len(df)) - 1

        print(f"\n MARKET REGIME ANALYSIS:")
        print(f"   • Average volatility: {volatility.mean():.1%}")
        print(f"   • Annualized trend: {trend:.1%}")
        print(f"   • Max drawdown period: {self._calculate_max_dd_period(df['Close'])}")

        # Problem identification
        problems = []
        if len(lower_touches) < 5:
            problems.append(" Too few lower band touches - bands may be too tight")
        if len(upper_touches) < 5:
            problems.append(" Too few upper band touches - strategy not active enough")
        if df['BB_Width'].mean() < 0.05:
            problems.append(" Bands too narrow - not capturing enough volatility")
        if abs(trend) > 0.15:
            problems.append(" Strong trending market - mean reversion may not work")

        print(f"\n IDENTIFIED PROBLEMS:")
        for problem in problems:
            print(f"   {problem}")

        return problems

    def _calculate_max_dd_period(self, prices):
        """Calculate maximum drawdown period"""
        peak = prices.expanding().max()
        dd = (prices - peak) / peak
        max_dd = dd.min()
        dd_start = dd[dd == max_dd].index[0]
        recovery = prices[dd_start:][prices[dd_start:] >= peak[dd_start]]
        if len(recovery) > 0:
            dd_end = recovery.index[0]
            return (dd_end - dd_start).days
        return "Ongoing"

    def enhanced_bollinger_strategy(self, df,
                                  bb_period=20,
                                  bb_std=2,
                                  rsi_period=14,
                                  volume_filter=True,
                                  trend_filter=True,
                                  position_sizing='fixed'):
        """
        Enhanced Bollinger Bands strategy with multiple filters
        """
        self.reset()
        df = df.copy()

        # Basic Bollinger Bands
        df['MA'] = df['Close'].rolling(bb_period).mean()
        df['STD'] = df['Close'].rolling(bb_period).std()
        df['UpperBand'] = df['MA'] + (bb_std * df['STD'])
        df['LowerBand'] = df['MA'] - (bb_std * df['STD'])
        df['BB_Position'] = (df['Close'] - df['LowerBand']) / (df['UpperBand'] - df['LowerBand'])
        df['BB_Width'] = (df['UpperBand'] - df['LowerBand']) / df['MA']

        # Additional filters
        # 1. RSI filter
        delta = df['Close'].diff()
        gains = delta.where(delta > 0, 0).ewm(alpha=1/rsi_period).mean()
        losses = (-delta.where(delta < 0, 0)).ewm(alpha=1/rsi_period).mean()
        rs = gains / losses
        df['RSI'] = 100 - (100 / (1 + rs))

        # 2. Volume filter
        df['Volume_MA'] = df['Volume'].rolling(20).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_MA']

        # 3. Trend filter
        df['Trend_MA'] = df['Close'].rolling(50).mean()
        df['Trend_Direction'] = np.where(df['Close'] > df['Trend_MA'], 1, -1)

        # 4. Volatility regime
        returns = df['Close'].pct_change()
        df['Volatility'] = returns.rolling(20).std()
        df['Vol_Regime'] = pd.qcut(df['Volatility'].fillna(df['Volatility'].mean()),
                                   q=3, labels=['Low', 'Medium', 'High'])

        position = 0
        entry_price = 0

        for i, row in df.iterrows():
            current_price = row['Close']
            current_date = i

            # Skip if we don't have enough data
            if pd.isna(row['UpperBand']) or pd.isna(row['RSI']):
                self.portfolio_value.append(self.cash + (self.shares * current_price))
                continue

            # Generate signals with filters
            # Buy conditions (enhanced)
            buy_conditions = [
                current_price <= row['LowerBand'],  # Basic BB signal
                row['RSI'] < 35 if trend_filter else True,  # RSI oversold
                row['Volume_Ratio'] > 1.2 if volume_filter else True,  # Volume confirmation
                row['Vol_Regime'] != 'Low' if len(df) > 60 else True,  # Avoid low vol periods
                position == 0  # No current position
            ]

            # Sell conditions (enhanced)
            sell_conditions = [
                current_price >= row['UpperBand'],  # Basic BB signal
                row['RSI'] > 65 if trend_filter else True,  # RSI overbought
                position == 1  # Have position
            ]

            # Alternative exit conditions
            stop_loss_conditions = [
                position == 1,
                (current_price - entry_price) / entry_price < -0.08  # 8% stop loss
            ]

            # Execute trades
            if all(buy_conditions):
                # Position sizing
                if position_sizing == 'fixed':
                    shares = self.cash // current_price
                elif position_sizing == 'volatility':
                    vol_adj = min(0.02 / (row['Volatility'] + 0.01), 1.0)  # Risk-adjusted sizing
                    shares = int((self.cash * vol_adj) // current_price)
                else:  # conservative
                    shares = int((self.cash * 0.8) // current_price)

                if shares > 0:
                    self.shares = shares
                    self.cash -= shares * current_price
                    position = 1
                    entry_price = current_price

                    self.trades.append({
                        'date': current_date,
                        'action': 'BUY',
                        'price': current_price,
                        'shares': shares,
                        'rsi': row['RSI'],
                        'bb_position': row['BB_Position'],
                        'volume_ratio': row['Volume_Ratio'],
                        'volatility': row['Volatility'],
                        'signal': 'Enhanced BB Buy'
                    })

            elif all(sell_conditions) or all(stop_loss_conditions):
                if position == 1:
                    self.cash += self.shares * current_price
                    profit = (current_price - entry_price) * self.shares

                    signal_type = 'Stop Loss' if all(stop_loss_conditions) else 'BB Sell'

                    self.trades.append({
                        'date': current_date,
                        'action': 'SELL',
                        'price': current_price,
                        'shares': self.shares,
                        'rsi': row['RSI'],
                        'bb_position': row['BB_Position'],
                        'profit': profit,
                        'signal': signal_type
                    })

                    self.shares = 0
                    position = 0
                    entry_price = 0

            # Track portfolio value
            current_value = self.cash + (self.shares * current_price)
            self.portfolio_value.append(current_value)

        return self.calculate_enhanced_performance(df)

    def calculate_enhanced_performance(self, df):
        """Calculate enhanced performance metrics"""
        if not self.portfolio_value:
            return {'error': 'No portfolio values'}

        portfolio_series = pd.Series(self.portfolio_value)
        returns = portfolio_series.pct_change().dropna()

        # Basic metrics
        final_value = portfolio_series.iloc[-1]
        total_return = (final_value - self.starting_cash) / self.starting_cash

        # Risk metrics
        sharpe = returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0
        sortino = returns.mean() / returns[returns < 0].std() * np.sqrt(252) if len(returns[returns < 0]) > 0 else 0

        # Drawdown analysis
        rolling_max = portfolio_series.expanding().max()
        drawdown = (portfolio_series - rolling_max) / rolling_max
        max_dd = drawdown.min()

        # Trade analysis
        profitable_trades = len([t for t in self.trades if t['action'] == 'SELL' and t.get('profit', 0) > 0])
        total_completed_trades = len([t for t in self.trades if t['action'] == 'SELL'])
        win_rate = profitable_trades / total_completed_trades if total_completed_trades > 0 else 0

        # Benchmark comparison (buy and hold)
        buy_hold_return = (df['Close'].iloc[-1] / df['Close'].iloc[0]) - 1

        results = {
            'starting_cash': self.starting_cash,
            'final_value': final_value,
            'total_return': total_return * 100,
            'buy_hold_return': buy_hold_return * 100,
            'alpha': (total_return - buy_hold_return) * 100,
            'total_trades': total_completed_trades,
            'win_rate': win_rate * 100,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'max_drawdown': max_dd * 100,
            'profit_factor': self._calculate_profit_factor(),
            'avg_trade_duration': self._calculate_avg_trade_duration(),
            'portfolio_values': self.portfolio_value
        }

        return results

    def _calculate_profit_factor(self):
        """Calculate profit factor (gross profits / gross losses)"""
        profits = sum([t.get('profit', 0) for t in self.trades if t.get('profit', 0) > 0])
        losses = abs(sum([t.get('profit', 0) for t in self.trades if t.get('profit', 0) < 0]))
        return profits / losses if losses > 0 else float('inf')

    def _calculate_avg_trade_duration(self):
        """Calculate average trade duration"""
        durations = []
        buy_trades = [t for t in self.trades if t['action'] == 'BUY']
        sell_trades = [t for t in self.trades if t['action'] == 'SELL']

        for i, sell_trade in enumerate(sell_trades):
            if i < len(buy_trades):
                duration = (sell_trade['date'] - buy_trades[i]['date']).days
                durations.append(duration)

        return np.mean(durations) if durations else 0

    def parameter_optimization(self, df, param_ranges=None):
        """
        Test different parameter combinations to find optimal settings
        """
        if param_ranges is None:
            param_ranges = {
                'bb_period': [15, 20, 25],
                'bb_std': [1.5, 2.0, 2.5],
                'rsi_period': [10, 14, 20]
            }

        results = []

        print(" PARAMETER OPTIMIZATION:")
        print("="*60)

        for bb_period in param_ranges['bb_period']:
            for bb_std in param_ranges['bb_std']:
                for rsi_period in param_ranges['rsi_period']:

                    # Test configuration
                    result = self.enhanced_bollinger_strategy(
                        df,
                        bb_period=bb_period,
                        bb_std=bb_std,
                        rsi_period=rsi_period,
                        volume_filter=True,
                        trend_filter=True
                    )

                    if 'error' not in result:
                        results.append({
                            'bb_period': bb_period,
                            'bb_std': bb_std,
                            'rsi_period': rsi_period,
                            'total_return': result['total_return'],
                            'sharpe_ratio': result['sharpe_ratio'],
                            'max_drawdown': result['max_drawdown'],
                            'win_rate': result['win_rate'],
                            'total_trades': result['total_trades']
                        })

        # Sort by risk-adjusted return
        results_df = pd.DataFrame(results)
        results_df['score'] = results_df['total_return'] * results_df['sharpe_ratio'] / abs(results_df['max_drawdown'] + 0.01)
        results_df = results_df.sort_values('score', ascending=False)

        print(" TOP 5 PARAMETER COMBINATIONS:")
        print(results_df.head().to_string(index=False))

        return results_df

def compare_strategies(df):
    """
    Compare original vs enhanced Bollinger Bands strategies
    """
    print(" STRATEGY COMPARISON")
    print("="*60)

    backtester = EnhancedBollingerBacktest(starting_cash=25000)

    # Analyze problems with current strategy
    problems = backtester.analyze_current_strategy_problems(df)

    # Test enhanced strategy
    print(f"\n TESTING ENHANCED STRATEGY:")
    enhanced_results = backtester.enhanced_bollinger_strategy(df)

    if 'error' not in enhanced_results:
        print(f"Enhanced Strategy Results:")
        print(f"   • Total Return: {enhanced_results['total_return']:.2f}%")
        print(f"   • Alpha vs Buy-Hold: {enhanced_results['alpha']:.2f}%")
        print(f"   • Sharpe Ratio: {enhanced_results['sharpe_ratio']:.2f}")
        print(f"   • Win Rate: {enhanced_results['win_rate']:.1f}%")
        print(f"   • Max Drawdown: {enhanced_results['max_drawdown']:.2f}%")
        print(f"   • Total Trades: {enhanced_results['total_trades']}")

        # Parameter optimization
        print(f"\n RUNNING PARAMETER OPTIMIZATION...")
        opt_results = backtester.parameter_optimization(df)

        return enhanced_results, opt_results
    else:
        print("Enhanced strategy failed to execute")
        return None, None

if __name__ == "__main__":
    # This would be called from your main script
    pass
